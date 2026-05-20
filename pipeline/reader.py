"""
reader.py — Reads the Delta Lake telemetry table into a list of TelemetrySnapshot.

Intentionally abstracted behind a simple interface so swapping in a PySpark DataFrame reader for production is a one-file change. The processor doesn't care how data got loaded.

Delta Lake note: we read the parquet files directly rather than using deltalake-py, since the dataset is a clean single-partition append-only table and we don't need time-travel or schema enforcement from the Delta log. For a multi-writer production
table, add `deltalake` to requirements and use `DeltaTable.to_pyarrow_dataset()`.
"""

from __future__ import annotations

import glob
import logging
from pathlib import Path
from typing import Iterator, List, Optional

import pyarrow.parquet as pq

from pipeline.models import (
    DistributionStats,
    PlayerStats,
    TelemetrySnapshot,
    TimestampInfo,
    TrafficStats,
)

logger = logging.getLogger(__name__)


def _parse_traffic_stats(raw: Optional[dict]) -> Optional[TrafficStats]:
    if raw is None:
        return None
    return TrafficStats(**raw)


def _parse_distribution(raw: Optional[dict]) -> Optional[DistributionStats]:
    if raw is None:
        return None
    return DistributionStats(
        sourceTraffic=_parse_traffic_stats(raw.get("sourceTraffic")),
        p2pTraffic=_parse_traffic_stats(raw.get("p2pTraffic")),
    )


def _parse_quality_distribution(
    raw: Optional[list],
) -> Optional[dict[str, DistributionStats]]:
    """
    PyArrow deserialises map<string, struct> as a list of (key, value) tuples.
    Convert to a proper dict here so the processor can just do a .get("1080p").
    """
    if not raw:
        return None
    result = {}
    for key, value in raw:
        result[key] = _parse_distribution(value)
    return result


def _row_to_snapshot(row: dict) -> TelemetrySnapshot:
    ts_raw = row.get("timestampInfo")
    player_raw = row.get("player")
    total_raw = row.get("totalDistribution")
    qual_raw = row.get("qualityDistribution")

    return TelemetrySnapshot(
        customerId=row.get("customerId"),
        contentId=row.get("contentId"),
        clientId=row.get("clientId"),
        eventDate=row.get("eventDate"),
        timestampInfo=TimestampInfo(**ts_raw) if ts_raw else None,
        player=PlayerStats(**player_raw) if player_raw else None,
        totalDistribution=_parse_distribution(total_raw),
        qualityDistribution=_parse_quality_distribution(qual_raw),
    )


def _read_parquet_file(path: str) -> Iterator[TelemetrySnapshot]:
    """Yields snapshots from a single parquet file. Skips malformed rows with a warning."""
    pf = pq.ParquetFile(path)
    table = pf.read()
    for row in table.to_pylist():
        try:
            yield _row_to_snapshot(row)
        except Exception as exc:
            # Don't let one bad row kill the batch — log and move on.
            # In production, route these to a dead-letter topic/table.
            logger.warning("Skipping malformed row: %s | error: %s", row, exc)


def load_snapshots(
    input_path: str,
    date_partition: Optional[str] = None,
) -> List[TelemetrySnapshot]:
    """
    Load all telemetry snapshots from the given Delta table path.

    Args:
        input_path: Root of the Delta table (the directory containing _delta_log/).
        date_partition: If given (e.g. "2025-11-13"), restrict to that partition.
                        Useful when running the pipeline incrementally per date.

    Returns:
        List of TelemetrySnapshot objects ready for processing.
    """
    base = Path(input_path)
    if not base.exists():
        raise FileNotFoundError(f"Input path does not exist: {base.resolve()}")

    # Build glob pattern. Partition directory is `eventDate=<date>`.
    if date_partition:
        pattern = str(base / f"eventDate={date_partition}" / "*.parquet")
    else:
        pattern = str(base / "eventDate=*" / "*.parquet")

    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No parquet files found matching: {pattern}"
        )

    logger.info("Loading %d parquet files from %s", len(files), base)

    snapshots: List[TelemetrySnapshot] = []
    for path in files:
        file_snapshots = list(_read_parquet_file(path))
        snapshots.extend(file_snapshots)
        logger.debug("  %s → %d rows", Path(path).name, len(file_snapshots))

    logger.info("Loaded %d total snapshots", len(snapshots))
    return snapshots
