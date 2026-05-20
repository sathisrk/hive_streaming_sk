"""
processor.py — All metric computation logic.

Pure functions throughout — no I/O, no side effects, no global state. This makes the math easy to unit test and means we can lift it directly into a Spark UDF or mapGroups() call without touching the logic.

The processing model is simple:
    snapshots -> group by (customerId, contentId, clientId) -> compute_viewer_metrics()

Each group is a time-ordered sequence of ~30s telemetry windows for one viewer session.
"""

from __future__ import annotations

import logging
from datetime import date
from itertools import groupby
from typing import Dict, List, Optional, Tuple

from pipeline.models import (
    QualityConsumption,
    TelemetrySnapshot,
    ViewerMetrics,
)

logger = logging.getLogger(__name__)



# Internal helpers



def _safe_int(val: Optional[int], default: int = 0) -> int:
    return val if val is not None else default


def _safe_float(val: Optional[float], default: float = 0.0) -> float:
    return val if val is not None else default


def _session_key(s: TelemetrySnapshot) -> Tuple[str, str, str]:
    """Group key: (customerId, contentId, clientId). All three are needed — a viewer
    can watch multiple events for the same customer, and we want separate rows."""
    return (
        s.customerId or "",
        s.contentId or "",
        s.clientId or "",
    )



# Per-viewer metric computation



def compute_viewer_metrics(
    snapshots: List[TelemetrySnapshot],
    heartbeat_interval_ms: int = 30_000,
) -> ViewerMetrics:
    """
    Compute QoS metrics for a single viewer session from their ordered snapshots.

    Args:
        snapshots: All telemetry snapshots for one (customer, content, client) triple.
                   Ordering by server_ts is done internally — callers don't need to sort.
        heartbeat_interval_ms: Expected interval between snapshots. Added to
                                (last_ts - first_ts) to estimate total watch time.

    Returns:
        ViewerMetrics with buffering and quality consumption metrics.
    """
    if not snapshots:
        raise ValueError("compute_viewer_metrics called with empty snapshots list")

    # Sort by server timestamp — safer than relying on file order.
    ordered = sorted(
        snapshots,
        key=lambda s: _safe_int(s.timestampInfo.server if s.timestampInfo else None),
    )

    first = ordered[0]
    last = ordered[-1]

    # --- Session identity ---
    customer_id = first.customerId or ""
    content_id = first.contentId or ""
    client_id = first.clientId or ""
    event_date: date = first.eventDate or date.today()

    # --- Session timing ---
    # Client confirmed: agent_ts = moment data is sent = END of the 30s reporting window.
    # That makes it the right timestamp for watch-time math — server_ts carries network
    # latency which would inflate session durations. We still order by server_ts (more
    # monotonic across viewers), but use agent_ts for the duration calculation.
    #
    # Given agent_ts marks the END of each window:
    #   watch_time = (last_agent_ts - first_agent_ts) + one_interval
    # The +interval accounts for the first window, which started interval_ms before its
    # agent_ts was emitted.
    session_start_ms = _safe_int(
        first.timestampInfo.server if first.timestampInfo else None
    )
    session_end_ms = _safe_int(
        last.timestampInfo.server if last.timestampInfo else None
    )
    first_agent_ts = _safe_int(
        first.timestampInfo.agent if first.timestampInfo else None
    ) or session_start_ms
    last_agent_ts = _safe_int(
        last.timestampInfo.agent if last.timestampInfo else None
    ) or session_end_ms

    watch_time_ms = max(
        (last_agent_ts - first_agent_ts) + heartbeat_interval_ms,
        heartbeat_interval_ms,  # floor at one interval for single-snapshot sessions
    )

    # --- Buffering ---
    total_buffering_events = sum(
        _safe_int(s.player.bufferings if s.player else None) for s in ordered
    )
    total_buffering_time_ms = sum(
        _safe_int(s.player.bufferingTime if s.player else None) for s in ordered
    )
    buffering_ratio = (
        total_buffering_time_ms / watch_time_ms if watch_time_ms > 0 else 0.0
    )
    avg_buffering_duration_ms: Optional[float] = (
        total_buffering_time_ms / total_buffering_events
        if total_buffering_events > 0
        else None
    )

    # --- Quality consumption ---
    # Aggregate bytes received per quality label across all snapshots.
    # We use receivedData (not requestedData) because that's what actually played.
    quality_bytes: Dict[str, int] = {}
    for snap in ordered:
        if not snap.qualityDistribution:
            continue
        for label, dist in snap.qualityDistribution.items():
            src_bytes = (
                _safe_int(dist.sourceTraffic.receivedData if dist.sourceTraffic else None)
            )
            p2p_bytes = (
                _safe_int(dist.p2pTraffic.receivedData if dist.p2pTraffic else None)
            )
            quality_bytes[label] = quality_bytes.get(label, 0) + src_bytes + p2p_bytes

    total_quality_bytes = sum(quality_bytes.values())
    quality_breakdown = [
        QualityConsumption(
            quality_label=label,
            received_bytes=b,
            share_pct=round(b / total_quality_bytes * 100, 2) if total_quality_bytes > 0 else 0.0,
        )
        # Sort descending by bytes so the dominant quality comes first in the list
        for label, b in sorted(quality_bytes.items(), key=lambda x: -x[1])
    ]
    dominant_quality = quality_breakdown[0].quality_label if quality_breakdown else None

    # --- Traffic split (source CDN vs P2P) ---
    # Client confirmed: P2P is set to 0 for this exercise. p2p_received_bytes will
    # always be 0, so p2p_ratio will always be 0.0. The fields are kept in the output
    # schema so the production pipeline can populate them without a schema migration.
    total_received_bytes = 0
    p2p_received_bytes = 0
    for snap in ordered:
        if not snap.totalDistribution:
            continue
        total_received_bytes += _safe_int(
            snap.totalDistribution.sourceTraffic.receivedData
            if snap.totalDistribution.sourceTraffic
            else None
        ) + _safe_int(
            snap.totalDistribution.p2pTraffic.receivedData
            if snap.totalDistribution.p2pTraffic
            else None
        )
        p2p_received_bytes += _safe_int(
            snap.totalDistribution.p2pTraffic.receivedData
            if snap.totalDistribution.p2pTraffic
            else None
        )

    p2p_ratio = (
        p2p_received_bytes / total_received_bytes if total_received_bytes > 0 else 0.0
    )

    return ViewerMetrics(
        customer_id=customer_id,
        content_id=content_id,
        client_id=client_id,
        event_date=event_date,
        session_start_ms=session_start_ms,
        session_end_ms=session_end_ms,
        watch_time_ms=watch_time_ms,
        snapshot_count=len(ordered),
        total_buffering_events=total_buffering_events,
        total_buffering_time_ms=total_buffering_time_ms,
        buffering_ratio=buffering_ratio,
        avg_buffering_duration_ms=avg_buffering_duration_ms,
        dominant_quality=dominant_quality,
        quality_breakdown=quality_breakdown,
        total_received_bytes=total_received_bytes,
        p2p_received_bytes=p2p_received_bytes,
        p2p_ratio=p2p_ratio,
    )



# Top-level pipeline function



def process_all(
    snapshots: List[TelemetrySnapshot],
    heartbeat_interval_ms: int = 30_000,
    min_snapshots: int = 1,
) -> List[ViewerMetrics]:
    """
    Group snapshots by viewer session and compute metrics for each.

    Args:
        snapshots: All loaded telemetry snapshots (mixed viewers/events).
        heartbeat_interval_ms: Forwarded to compute_viewer_metrics.
        min_snapshots: Drop viewers with fewer snapshots than this. Useful
                        for filtering out viewers who barely connected.

    Returns:
        List of ViewerMetrics, one per unique (customer, content, client).
    """
    if not snapshots:
        logger.warning("No snapshots to process — returning empty result")
        return []

    # Group — sorted() first because itertools.groupby needs consecutive equal keys.
    sorted_snapshots = sorted(snapshots, key=_session_key)
    results: List[ViewerMetrics] = []

    for key, group_iter in groupby(sorted_snapshots, key=_session_key):
        group = list(group_iter)

        if len(group) < min_snapshots:
            logger.debug(
                "Skipping client %s — only %d snapshot(s), below min_snapshots=%d",
                key[2],
                len(group),
                min_snapshots,
            )
            continue

        try:
            metrics = compute_viewer_metrics(group, heartbeat_interval_ms)
            results.append(metrics)
        except Exception as exc:
            # Bad data for one viewer shouldn't kill the whole run.
            logger.error("Failed to compute metrics for %s: %s", key, exc, exc_info=True)

    logger.info(
        "Processed %d viewer sessions from %d snapshots", len(results), len(snapshots)
    )
    return results
