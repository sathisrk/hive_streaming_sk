"""
writer.py — Writes ViewerMetrics to output sinks.

Currently supports JSON (primary) and CSV (for BI tools). Adding a Parquet sink for Delta-append is straightforward — just add a write_parquet() function following the same pattern.
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, List

from pipeline.models import ViewerMetrics

logger = logging.getLogger(__name__)


def _metrics_to_dict(m: ViewerMetrics) -> dict[str, Any]:
    """
    Flatten ViewerMetrics to a JSON-serialisable dict.

    The nested quality_breakdown list is kept as-is for JSON (it's useful there)
    and flattened per-quality for CSV.
    """
    base = m.model_dump()
    # Pydantic serialises date as a date object — JSON needs a string.
    base["event_date"] = str(m.event_date)
    return base


def write_json(metrics: List[ViewerMetrics], output_path: str) -> None:
    """Write all viewer metrics to a single JSON array file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    records = [_metrics_to_dict(m) for m in metrics]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)

    logger.info("Wrote %d viewer records to %s", len(metrics), path)


def write_csv(metrics: List[ViewerMetrics], output_path: str) -> None:
    """
    Write a flat CSV — one row per viewer, with one column per quality level.

    Quality columns are dynamic (depends on what labels appear in the data).
    Missing quality levels for a viewer get 0.
    """
    if not metrics:
        logger.warning("No metrics to write to CSV")
        return

    # Collect all quality labels across all viewers
    all_labels = sorted(
        {qb.quality_label for m in metrics for qb in m.quality_breakdown}
    )

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    base_fields = [
        "customer_id", "content_id", "client_id", "event_date",
        "session_start_ms", "session_end_ms", "watch_time_ms", "snapshot_count",
        "total_buffering_events", "total_buffering_time_ms",
        "buffering_ratio", "avg_buffering_duration_ms",
        "dominant_quality",
        "total_received_bytes", "p2p_received_bytes", "p2p_ratio",
    ]
    quality_bytes_fields = [f"quality_bytes_{label}" for label in all_labels]
    quality_pct_fields = [f"quality_pct_{label}" for label in all_labels]
    fieldnames = base_fields + quality_bytes_fields + quality_pct_fields

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for m in metrics:
            # Build quality lookup for this viewer
            qual_bytes = {qb.quality_label: qb.received_bytes for qb in m.quality_breakdown}
            qual_pct = {qb.quality_label: qb.share_pct for qb in m.quality_breakdown}

            row = {
                "customer_id": m.customer_id,
                "content_id": m.content_id,
                "client_id": m.client_id,
                "event_date": str(m.event_date),
                "session_start_ms": m.session_start_ms,
                "session_end_ms": m.session_end_ms,
                "watch_time_ms": m.watch_time_ms,
                "snapshot_count": m.snapshot_count,
                "total_buffering_events": m.total_buffering_events,
                "total_buffering_time_ms": m.total_buffering_time_ms,
                "buffering_ratio": round(m.buffering_ratio, 4),
                "avg_buffering_duration_ms": (
                    round(m.avg_buffering_duration_ms, 1)
                    if m.avg_buffering_duration_ms is not None
                    else ""
                ),
                "dominant_quality": m.dominant_quality or "",
                "total_received_bytes": m.total_received_bytes,
                "p2p_received_bytes": m.p2p_received_bytes,
                "p2p_ratio": round(m.p2p_ratio, 4),
            }
            for label in all_labels:
                row[f"quality_bytes_{label}"] = qual_bytes.get(label, 0)
                row[f"quality_pct_{label}"] = qual_pct.get(label, 0.0)

            writer.writerow(row)

    logger.info("Wrote %d viewer records to %s", len(metrics), path)
