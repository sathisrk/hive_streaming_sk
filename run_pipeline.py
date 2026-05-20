#!/usr/bin/env python3
"""
run_pipeline.py — CLI entry point for the viewer telemetry pipeline.

Usage:
    python run_pipeline.py --config config.yaml
    python run_pipeline.py --config config.yaml --date 2025-11-14
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path

import yaml

from pipeline.processor import process_all
from pipeline.reader import load_snapshots
from pipeline.writer import write_csv, write_json


def setup_logging(config: dict) -> None:
    log_cfg = config.get("logging", {})
    logging.basicConfig(
        level=getattr(logging, log_cfg.get("level", "INFO")),
        format=log_cfg.get("format", "%(asctime)s [%(levelname)s] %(name)s - %(message)s"),
    )


def load_config(config_path: str) -> dict:
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Hive Streaming viewer telemetry pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--date", default=None, help="Process only this date partition (YYYY-MM-DD)")
    parser.add_argument("--input", default=None, help="Override input_path from config")
    parser.add_argument("--output", default=None, help="Override output_path from config")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    setup_logging(config)

    pipeline_cfg = config.get("pipeline", {})
    input_path = args.input or pipeline_cfg.get("input_path", "./data")
    output_path = args.output or pipeline_cfg.get("output_path", "./output")
    heartbeat_interval_ms = pipeline_cfg.get("heartbeat_interval_ms", 30_000)
    min_snapshots = pipeline_cfg.get("min_snapshots", 1)

    logger = logging.getLogger("run_pipeline")
    logger.info("Starting pipeline | input=%s | date=%s", input_path, args.date or "all")

    try:
        snapshots = load_snapshots(input_path, date_partition=args.date)
    except FileNotFoundError as exc:
        logger.error("Could not load data: %s", exc)
        return 1

    if not snapshots:
        logger.warning("No data found - nothing to process")
        return 0

    metrics = process_all(
        snapshots,
        heartbeat_interval_ms=heartbeat_interval_ms,
        min_snapshots=min_snapshots,
    )

    if not metrics:
        logger.warning("Processing produced no output - check min_snapshots config")
        return 0

    output_dir = Path(output_path)
    write_json(metrics, str(output_dir / "viewer_metrics.json"))
    write_csv(metrics, str(output_dir / "viewer_metrics.csv"))

    logger.info("Pipeline complete. %d viewer sessions written to %s", len(metrics), output_dir)

    buffering_viewers = sum(1 for m in metrics if m.total_buffering_events > 0)
    avg_ratio = sum(m.buffering_ratio for m in metrics) / len(metrics)
    dom_quality_dist = Counter(m.dominant_quality for m in metrics if m.dominant_quality)

    print(f"\n{'='*50}")
    print(f"  Pipeline Summary")
    print(f"{'='*50}")
    print(f"  Viewers processed:     {len(metrics)}")
    print(f"  With buffering:        {buffering_viewers} ({100*buffering_viewers/len(metrics):.0f}%)")
    print(f"  Avg buffering ratio:   {avg_ratio*100:.1f}%")
    print(f"  Dominant quality dist: {dict(dom_quality_dist.most_common())}")
    print(f"{'='*50}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
