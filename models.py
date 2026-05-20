"""
models.py — Pydantic schemas for the telemetry pipeline.

Keeping input and output schemas explicit here pays dividends: any upstream schema change surfaces at parse time, not buried in a KeyError at 2am.
"""

from __future__ import annotations

from datetime import date
from typing import Dict, Optional

from pydantic import BaseModel, Field, model_validator


# Input schema (mirrors the Delta table schema)


class TrafficStats(BaseModel):
    requests: Optional[int] = None
    responses: Optional[float] = None
    requestedData: Optional[int] = None
    receivedData: Optional[int] = None


class DistributionStats(BaseModel):
    sourceTraffic: Optional[TrafficStats] = None
    p2pTraffic: Optional[TrafficStats] = None


class TimestampInfo(BaseModel):
    server: Optional[int] = None  # Unix ms, server-side receipt time
    agent: Optional[int] = None   # Unix ms, client-side send time


class PlayerStats(BaseModel):
    bufferings: Optional[int] = None      # # buffering events in this window
    bufferingTime: Optional[int] = None   # total buffering ms in this window


class TelemetrySnapshot(BaseModel):
    """One 30-second heartbeat from a viewer's player."""

    customerId: Optional[str] = None
    contentId: Optional[str] = None
    clientId: Optional[str] = None
    eventDate: Optional[date] = None
    timestampInfo: Optional[TimestampInfo] = None
    player: Optional[PlayerStats] = None
    totalDistribution: Optional[DistributionStats] = None
    # map<quality_label, DistributionStats> — e.g. "1080p", "720p", etc.
    qualityDistribution: Optional[Dict[str, DistributionStats]] = None


# Output schema (one row per viewer session)


class QualityConsumption(BaseModel):
    """Bytes received at a given quality level, plus derived share."""

    quality_label: str
    received_bytes: int = 0
    share_pct: float = Field(0.0, ge=0.0, le=100.0)


class ViewerMetrics(BaseModel):
    """Per-viewer QoS summary for one event session."""

    # Identity
    customer_id: str
    content_id: str
    client_id: str
    event_date: date

    # Session span
    session_start_ms: int   # server timestamp of first snapshot
    session_end_ms: int     # server timestamp of last snapshot
    # Watch time = (end - start) + heartbeat_interval. See README assumptions.
    watch_time_ms: int

    # Snapshot count — useful for debugging sparse sessions
    snapshot_count: int

    # Buffering
    total_buffering_events: int
    total_buffering_time_ms: int
    # Fraction of watch time spent buffering. 0.05 = 5%.
    buffering_ratio: float = Field(ge=0.0)
    # Mean duration per buffering event (ms). None if no buffering occurred.
    avg_buffering_duration_ms: Optional[float] = None

    # Quality consumption
    dominant_quality: Optional[str] = None   # quality with most bytes received
    quality_breakdown: list[QualityConsumption] = Field(default_factory=list)

    # Traffic distribution
    total_received_bytes: int = 0
    p2p_received_bytes: int = 0
    # P2P ratio = p2p_bytes / total_bytes. Relevant for Hive's P2P delivery layer.
    p2p_ratio: float = Field(0.0, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _clamp_buffering_ratio(self) -> "ViewerMetrics":
        # Floating point slop can push this slightly above 1.0 in edge cases
        # (e.g. viewer buffers for longer than their measured watch time).
        # Cap at 1.0 rather than blowing up downstream consumers.
        self.buffering_ratio = min(self.buffering_ratio, 1.0)
        return self
