# Architecture Decision Record — Telemetry Pipeline

## Scale context

| Metric | Value | Source |
| Peak concurrent viewers | ~100,000 | Client |
| Heartbeat interval | 30s | Dataset |
| Peak ingest rate | ~3,333 msg/s | 100k / 30s |
| Records per 2h webcast | ~24M | 100k × 240 windows |
| Quality levels | 4 (270p/360p/720p/1080p) | Dataset |
| P2P traffic | 0 (future) | Client |

## Latency decision: 30–60 seconds end-to-end

**Chosen:** Near-real-time, one micro-batch per heartbeat interval.

**Reasoning:** This is a live webcast monitoring system. If 15% of viewers hit a buffering spike at 14:32, ops needs to know by 14:33 — not 14:37.
A 5-minute lag means the problem could compound across the entire audience before anyone sees it in a dashboard. The 30s trigger interval aligns naturally with the heartbeat cadence, so each batch is a clean, complete window — no partial-window state management needed.

5-minute or hourly batch would be simpler to operate but defeats the purpose of a monitoring system for live events.

## Architecture diagram

```
HLS.js Player (browser)
    │  30s heartbeat (JSON)
API Gateway / Load Balancer
    │  fan-in from 100k viewers
Kafka  ──────────────────────────────────────────────────────────────────
  Topic: telemetry-raw                                                   │
  Partitions: 16  (scales linearly — add partitions for >500k viewers)  │
  Retention: 24h  (enough for a full-day replay if processing falls      │
                   behind; longer if you need backfill)                  │
    │                                                                     │
Spark Structured Streaming  ◄────────────────────────────────────────────
  Trigger: 30 seconds
  Executors: 10-20 × m5.xlarge (scales to ~1M concurrent viewers)
  Logic: compute_metrics() from spark_processor.py
    │
    ├── Delta Lake: viewer_metrics/
    │      Partitioned by event_date
    │      ZORDERed by content_id, client_id
    │      (fast "all viewers for event X" queries)
    │
    └──(optional) ClickHouse / Druid / Pinot
           For sub-second dashboard query latency
           Feed from Delta CDC or a second Kafka topic
```

## Volume the code can handle

| Tier | Implementation | Max concurrent viewers |
| Single-node pandas | `processor.py` | ~10,000 |
| PySpark batch | `spark_processor.py` (batch) | ~500,000 |
| PySpark streaming | `spark_processor.py` (streaming) | ~1,000,000 |
| Beyond 1M | Replace Spark with Flink + Iceberg | Unlimited (horizontal) |

The jump from batch to streaming adds operational overhead (checkpoint management, Kafka ops). For events under ~50k viewers, the micro-batch pandas version running every 30s on a cron schedule is simpler and fine.

## What would change with raw events (vs pre-aggregated)

The current schema gives us buffering counts/totals per 30s window. With raw events (bufferingStart, bufferingEnd, qualitySwitch timestamps)
we could additionally compute:

- Exact position in the stream when buffering occurred (early vs late)
- Buffering clustering — did all viewers buffer at the same second? (CDN issue)
- Quality switch frequency and direction (upshift vs downshift patterns)
- Per-segment failure rates

This would require a stateful streaming operator (Spark mapGroupsWithState or Flink's keyed process functions) to correlate events across windows.

## P2P integration path

P2P fields are zeroed in this implementation per client confirmation. When the P2P SDK is integrated, the collector needs two additional hooks:

1. `p2p_sdk.onSegmentReceived(bytes, fromPeer)` → increment p2pTraffic.receivedData
2. `p2p_sdk.onSegmentRequested(bytes, toPeer)` → increment p2pTraffic.requestedData

No schema migration needed — the fields already exist in the output table.
