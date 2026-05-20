"""
spark_processor.py — PySpark implementation of the viewer telemetry pipeline.

Scale context (from client):
  - Peak concurrent viewers: ~100,000 per event
  - Heartbeat interval: 30s
  - Sustained ingest rate: 100,000 / 30 ≈ 3,333 messages/second at peak
  - Per-event total (2h webcast): ~24M records

This rate is well within PySpark Structured Streaming's comfort zone — a 10-executor cluster on m5.xlarge nodes handles this with headroom. The
bottleneck at scale is almost always the ingest path (Kafka broker sizing)
and the output sink (Delta write amplification), not the compute layer.
Latency decision: 30-60 seconds end-to-end.
Rationale: during a live webcast, ops teams need to detect buffering spikes within one reporting interval so they can act before the problem compounds.
A 5-minute lag means thousands of viewers could hit degraded QoS undetected.
The 30s micro-batch trigger aligns naturally with the 30s heartbeat cadence.

Architecture at 100k viewers:
  Kafka (3 brokers, 16 partitions)
    → Spark Structured Streaming (10-20 executors, 30s trigger)
    → Delta Lake (partitioned by event_date + content_id)
    → Dashboard / alert layer

  The pandas pipeline (processor.py) handles up to ~10k concurrent viewers
  on a single node. Beyond that, this file is the path forward.

Design notes:
  - Zero Python UDFs — all logic is native DataFrame transforms so Catalyst can optimize freely. Only exception is explode_outer on the quality map, which is unavoidable but still runs in JVM.
  - foreachBatch for streaming reuses the same batch compute_metrics() — no duplicated logic.
  - Writes are .mode("overwrite").partitionBy("event_date") — idempotent for backfill. For live incremental, swap to Delta MERGE on (client_id, event_date).
  - P2P fields are zeroed per client confirmation. Schema is kept intact for future population without a migration.

Usage:
    # Batch — one date partition
    spark-submit pipeline/spark_processor.py \\
        --input s3://bucket/telemetry-delta/ \\
        --output s3://bucket/viewer-metrics-delta/ \\
        --date 2025-11-13

    # Streaming micro-batch (production path)
    spark-submit pipeline/spark_processor.py \\
        --input s3://bucket/telemetry-delta/ \\
        --output s3://bucket/viewer-metrics-delta/ \\
        --checkpoint s3://bucket/checkpoints/telemetry/ \\
        --streaming
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)



# Spark session



def get_spark(app_name: str = "hive-telemetry-pipeline") -> SparkSession:
    """
    Returns the ambient session in Databricks/EMR, or spins up local[*] otherwise.
    Delta config is a no-op if the jar isn't on the classpath.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # AQE is on by default in Spark 3+ but worth being explicit — it helps
        # a lot with the skewed groupBy when one event has 100k viewers vs another
        # with 100. Without AQE those small-event tasks would sit idle waiting.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )



# Reading



def read_telemetry(
    spark: SparkSession,
    input_path: str,
    date_partition: Optional[str] = None,
) -> DataFrame:
    """
    Read from Delta. Partition filter triggers directory-level pruning — Spark only scans eventDate=<date>/ rather than the full table. At 24M records/event
    over months of history, skipping partitions is the difference between a 10s and a 10min job.
    """
    df = spark.read.format("delta").load(input_path)
    if date_partition:
        df = df.filter(F.col("eventDate").cast("string") == date_partition)
    logger.info("Loaded telemetry from %s%s",
                input_path,
                f" (eventDate={date_partition})" if date_partition else "")
    return df



# Metric computation — batch



def _buffering_metrics(df: DataFrame, heartbeat_ms: int) -> DataFrame:
    """
    Per-viewer buffering aggregation.

    Uses agent_ts (confirmed = end of 30s window) for watch_time math.
    server_ts is used for ordering only — it's more monotonic but carries network latency that would inflate durations.
    """
    return (
        df
        .select(
            "customerId", "contentId", "clientId", "eventDate",
            F.col("timestampInfo.server").alias("server_ts"),
            F.col("timestampInfo.agent").alias("agent_ts"),
            F.coalesce(F.col("player.bufferings"),    F.lit(0)).alias("bufferings"),
            F.coalesce(F.col("player.bufferingTime"), F.lit(0)).alias("bufferingTime"),
            # P2P confirmed as 0 for this exercise — fields kept for schema compatibility
            F.coalesce(F.col("totalDistribution.sourceTraffic.receivedData"), F.lit(0)).alias("src_bytes"),
            F.coalesce(F.col("totalDistribution.p2pTraffic.receivedData"),    F.lit(0)).alias("p2p_bytes"),
        )
        .groupBy("customerId", "contentId", "clientId", "eventDate")
        .agg(
            F.min("server_ts").alias("session_start_ms"),
            F.max("server_ts").alias("session_end_ms"),
            # agent_ts for accurate watch-time — it's the end of each window
            F.min("agent_ts").alias("first_agent_ts"),
            F.max("agent_ts").alias("last_agent_ts"),
            F.count("*").alias("snapshot_count"),
            F.sum("bufferings").alias("total_buffering_events"),
            F.sum("bufferingTime").alias("total_buffering_time_ms"),
            (F.sum("src_bytes") + F.sum("p2p_bytes")).alias("total_received_bytes"),
            F.sum("p2p_bytes").alias("p2p_received_bytes"),
        )
        # watch_time = (last_window_end - first_window_end) + one_interval
        # The +interval accounts for the first window starting interval_ms before its agent_ts
        .withColumn(
            "watch_time_ms",
            (F.col("last_agent_ts") - F.col("first_agent_ts")) + F.lit(heartbeat_ms),
        )
        .withColumn(
            "buffering_ratio",
            # Cap at 1.0 — noisy data or clock skew can push past it
            F.least(
                F.lit(1.0),
                F.col("total_buffering_time_ms").cast("double") /
                F.greatest(F.col("watch_time_ms").cast("double"), F.lit(1.0)),
            ),
        )
        .withColumn(
            "avg_buffering_duration_ms",
            F.when(
                F.col("total_buffering_events") > 0,
                F.col("total_buffering_time_ms").cast("double") /
                F.col("total_buffering_events").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "p2p_ratio",
            F.when(
                F.col("total_received_bytes") > 0,
                F.col("p2p_received_bytes").cast("double") /
                F.col("total_received_bytes").cast("double"),
            ).otherwise(F.lit(0.0)),
        )
    )


def _quality_metrics(df: DataFrame) -> DataFrame:
    """
    Dominant quality and per-quality byte breakdown from qualityDistribution map.

    qualityDistribution is map<string, struct<sourceTraffic, p2pTraffic>>.
    explode_outer turns each map entry into a separate row so we can aggregate normally. Avoids the "collect into a UDF and loop" antipattern that's everywhere in older Spark code.

    At 100k viewers with 4 quality levels each, explode produces ~400k rows
    per batch — still well within a single shuffle.
    """
    exploded = (
        df.select(
            "customerId", "contentId", "clientId", "eventDate",
            F.explode_outer("qualityDistribution").alias("quality_label", "dist"),
        )
        .select(
            "customerId", "contentId", "clientId", "eventDate", "quality_label",
            (
                F.coalesce(F.col("dist.sourceTraffic.receivedData"), F.lit(0)) +
                F.coalesce(F.col("dist.p2pTraffic.receivedData"),    F.lit(0))
            ).cast("long").alias("quality_bytes"),
        )
        .groupBy("customerId", "contentId", "clientId", "eventDate", "quality_label")
        .agg(F.sum("quality_bytes").alias("total_quality_bytes"))
    )

    w = Window.partitionBy("customerId", "contentId", "clientId", "eventDate")

    with_stats = (
        exploded
        .withColumn("viewer_total_bytes", F.sum("total_quality_bytes").over(w))
        .withColumn(
            "share_pct",
            F.round(
                F.col("total_quality_bytes").cast("double") /
                F.greatest(F.col("viewer_total_bytes").cast("double"), F.lit(1.0)) * 100.0,
                2,
            ),
        )
        .withColumn("quality_rank",
            F.rank().over(w.orderBy(F.desc("total_quality_bytes"))))
    )

    dominant = (
        with_stats.filter(F.col("quality_rank") == 1)
        .select(
            "customerId", "contentId", "clientId", "eventDate",
            F.col("quality_label").alias("dominant_quality"),
        )
    )

    quality_map = (
        with_stats
        .groupBy("customerId", "contentId", "clientId", "eventDate")
        .agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("quality_label").alias("key"),
                        F.struct(
                            F.col("total_quality_bytes").alias("received_bytes"),
                            F.col("share_pct"),
                        ).alias("value"),
                    )
                )
            ).alias("quality_breakdown")
        )
    )

    return dominant.join(
        quality_map, on=["customerId", "contentId", "clientId", "eventDate"], how="left"
    )


def compute_metrics(df: DataFrame, heartbeat_interval_ms: int = 30_000) -> DataFrame:
    """
    Orchestrate buffering + quality computation. Returns one row per viewer session.
    """
    buf = _buffering_metrics(df, heartbeat_interval_ms)
    qua = _quality_metrics(df)
    return (
        buf.join(qua, on=["customerId", "contentId", "clientId", "eventDate"], how="left")
        .select(
            F.col("customerId").alias("customer_id"),
            F.col("contentId").alias("content_id"),
            F.col("clientId").alias("client_id"),
            F.col("eventDate").alias("event_date"),
            "session_start_ms", "session_end_ms", "watch_time_ms", "snapshot_count",
            "total_buffering_events", "total_buffering_time_ms",
            F.round("buffering_ratio", 6).alias("buffering_ratio"),
            "avg_buffering_duration_ms",
            "dominant_quality", "quality_breakdown",
            "total_received_bytes", "p2p_received_bytes",
            F.round("p2p_ratio", 6).alias("p2p_ratio"),
        )
    )



# Writing



def write_metrics(df: DataFrame, output_path: str, fmt: str = "delta") -> None:
    """
    Partition by event_date for efficient downstream reads.
    Overwrite makes backfill reruns safe.

    For production UPSERT semantics on a live table:
        from delta.tables import DeltaTable
        DeltaTable.forPath(spark, output_path).alias("t")
            .merge(df.alias("s"),
                   "t.client_id = s.client_id AND t.event_date = s.event_date")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()

    ZORDER recommendation for the output table:
        OPTIMIZE viewer_metrics ZORDER BY (content_id, client_id)
    This collocates all viewers for one event on the same files, which is the
    dominant query pattern (give me all viewers for event X).
    """
    (
        df.write
        .format(fmt)
        .mode("overwrite")
        .partitionBy("event_date")
        .option("overwriteSchema", "true")
        .save(output_path)
    )
    logger.info("Metrics written to %s (format=%s)", output_path, fmt)



# Streaming variant



def run_streaming(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "30 seconds",
    heartbeat_ms: int = 30_000,
) -> None:
    """
    Structured Streaming via foreachBatch.

    30s trigger aligns with the 30s heartbeat — each batch represents one
    reporting window across all active viewers. At 100k viewers this is
    ~100k rows per batch, which Spark processes in well under 30s on a
    modest cluster, keeping us latency-bound rather than throughput-bound.

    Checkpoint guarantees exactly-once Delta file processing on restart.
    For multi-region failover, put the checkpoint on S3/ADLS, not local disk.

    Volume the cluster can handle with this config:
      - 10 x m5.xlarge executors (4 cores, 16GB each): handles up to ~500k viewers
      - 20 x m5.xlarge: ~1M viewers with headroom
      - Beyond 1M: consider Flink for lower per-event overhead, or increase
        trigger interval to 60s to reduce shuffle frequency
    """
    raw_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")  # safe for append-only sources
        .load(input_path)
    )

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        logger.info("Streaming batch_id=%d", batch_id)
        if batch_df.rdd.isEmpty():
            return
        write_metrics(compute_metrics(batch_df, heartbeat_ms), output_path, fmt="delta")

    (
        raw_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
        .awaitTermination()
    )



# CLI



def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Hive Streaming telemetry — PySpark pipeline")
    p.add_argument("--input",       required=True)
    p.add_argument("--output",      required=True)
    p.add_argument("--date",        default=None, help="eventDate partition (YYYY-MM-DD)")
    p.add_argument("--heartbeat-interval-ms", type=int, default=30_000)
    p.add_argument("--format",      default="delta", choices=["delta", "parquet", "json"])
    p.add_argument("--streaming",   action="store_true")
    p.add_argument("--checkpoint",  default=None)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.streaming:
        if not args.checkpoint:
            sys.exit("--checkpoint is required in streaming mode")
        run_streaming(spark, args.input, args.output, args.checkpoint,
                      heartbeat_ms=args.heartbeat_interval_ms)
    else:
        df = read_telemetry(spark, args.input, date_partition=args.date)
        metrics = compute_metrics(df, heartbeat_interval_ms=args.heartbeat_interval_ms)
        write_metrics(metrics, args.output, fmt=args.format)

        metrics.groupBy("dominant_quality").count().orderBy(F.desc("count")).show()
        metrics.select(
            F.count("*").alias("total_viewers"),
            F.round(F.avg("buffering_ratio") * 100, 2).alias("avg_buf_ratio_pct"),
            F.round(F.avg("p2p_ratio") * 100, 2).alias("avg_p2p_pct"),
        ).show()


if __name__ == "__main__":
    main()
