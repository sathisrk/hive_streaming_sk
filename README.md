# Hive Streaming

## Overview

Batch/micro-batch pipeline that processes viewer telemetry from a Delta Lake table and produces per-viewer QoS metrics: buffering behaviour and video quality consumption.

The input is a partitioned Delta table of 30-second telemetry snapshots (one row per viewer per interval). The output is a flat metrics table — one row per viewer session — plus a self-contained HTML dashboard.

## Dataset & Schema

| Field | Type | Description |
| `customerId` | string | Enterprise customer UUID |
| `contentId` | string | Event / webcast UUID |
| `clientId` | string | Viewer session UUID |
| `eventDate` | date | Partition key |
| `timestampInfo.server` | long (ms) | Backend-received timestamp |
| `timestampInfo.agent` | long (ms) | Client-sent timestamp |
| `player.bufferings` | int | # buffering events in this 30s window |
| `player.bufferingTime` | int | Total ms spent buffering in this window |
| `totalDistribution` | struct | Source + P2P traffic totals |
| `qualityDistribution` | map<str, struct> | Per-quality traffic breakdown |

**Key observations about the data:**
- Each row is a 30s telemetry heartbeat, not a raw event stream
- `bufferings` and `bufferingTime` are counts/durations for *that interval*, not cumulative
- Quality keys observed: `270p`, `360p`, `720p`, `1080p`
- `receivedData` is used as a proxy for quality consumption duration (bytes ∝ time at bitrate)
- The dataset covers a single event (~14 min, 20 viewers, 1 customer)
- Timestamps are Unix epoch milliseconds

## Assumptions

1. **Watch time** = `(max_server_ts - min_server_ts) + 30_000ms` per viewer. The +30s accounts for the last reporting interval. This is an underestimate if a viewer joined mid-interval.

2. **Quality time distribution** is proxied by `receivedData` bytes, not explicit duration.
   This is the only available signal. At constant bitrate, bytes ∝ duration, which holds well for HLS fixed-bitrate segments.

3. **P2P ratio** = p2p receivedData / (p2p + source receivedData). Included because it's core to Hive's value prop and interesting for QoS context.

4. **Each parquet file is treated as independent** — no deduplication is applied since the Delta log shows no UPDATE/DELETE operations, only APPENDs. If re-runs produce duplicates,
   add a `clientId + server_ts` dedup step in `processor.py`.

5. **"Stream processing"** in the assignment context maps to micro-batch here (one partition per run). In a real deployment this would be Spark Structured Streaming or Flink reading from Kafka, with the same metric logic applied per watermarked window.

## Project Structure

```
hive-telemetry-pipeline/
README.md
requirements.txt
config.yaml              # All env-specific values live here, not in code
pipeline/
   models.py            # Pydantic I/O schemas
   reader.py            # Delta/Parquet reader (abstracted for easy swap to Spark)
   processor.py         # All metric computation logic — pure functions, easy to test
   writer.py            # Output sink abstraction
dashboard/
   index.html           # Self-contained QoS dashboard (no build step)
run_pipeline.py          # CLI entry point
output/                  # Pipeline output (add to .gitignore in real repo)
```

## Setup & Run

```bash
pip install -r requirements.txt

# Run against the provided dataset
python run_pipeline.py --config config.yaml

# Point at a different date partition
python run_pipeline.py --config config.yaml --date 2025-11-14

```

## Output

Two files in `output/`:
- `viewer_metrics.json` — per-viewer QoS metrics (primary output)
- `viewer_metrics.csv` — same data, flat CSV for easy import into BI tools


## Scaling to Production

This pipeline runs fine on a single node for events with thousands of viewers.
For millions:
- Swap `reader.py` for a PySpark reader (same interface, same `processor.py` logic)
- Partition output by `customerId` + `contentId` to avoid full-table scans downstream
- Add a Kafka source adapter and run as Spark Structured Streaming with a 30s trigger interval
- The metric functions in `processor.py` are already designed as reduce operations,
  which maps cleanly to Spark's `groupBy().agg()` pattern

