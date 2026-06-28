"""Batch vs streaming reconciliation for the Sprint 6 MVP.

For a given ingest_hour, compare:
    - silver.events_push (batch) row count + commit_size sum
    - silver_streaming.events_push (streaming) row count + commit_size sum

Target: row-count delta < 0.01 % (i.e. < 1 row per 10,000).

This is the headline "batch + streaming story" senior-signal artifact
(per PROJECT_PLAN.md's seven signals).
"""

from __future__ import annotations

import argparse
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BATCH_PATH = "dbt/spark-warehouse/silver.db/events_push"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("oss_pulse_reconcile")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "4g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-hour", required=True, help="e.g. 2025-01-15-12")
    parser.add_argument(
        "--streaming-path", default="data/streaming/silver_streaming/events_push"
    )
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=0.01,
        help="max allowed |stream - batch| / batch * 100 (default 0.01)",
    )
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    batch = (
        spark.read.format("delta")
        .load(BATCH_PATH)
        .filter(F.col("ingest_hour") == args.ingest_hour)
    )
    stream = spark.read.format("delta").load(args.streaming_path)

    batch_count = batch.count()
    stream_count = stream.count()
    batch_commits = batch.agg(F.sum("commit_size")).collect()[0][0] or 0
    stream_commits = stream.agg(F.sum("commit_size")).collect()[0][0] or 0

    abs_delta = abs(stream_count - batch_count)
    pct_delta = (abs_delta / batch_count * 100) if batch_count else 0.0

    print("\n========== batch ↔ streaming reconciliation ==========")
    print(f"ingest_hour:       {args.ingest_hour}")
    print(f"batch rows:        {batch_count:,}")
    print(f"streaming rows:    {stream_count:,}")
    print(f"row count delta:   {stream_count - batch_count:+,} ({pct_delta:.4f}%)")
    print(f"batch commits Σ:   {batch_commits:,}")
    print(f"streaming commits: {stream_commits:,}")
    print(f"commits delta:     {stream_commits - batch_commits:+,}")

    # Set difference on (id) — what ids are present in one and not the
    # other. If the streaming consumer is healthy, these should both
    # be 0 modulo any in-flight messages.
    only_in_batch = batch.select("id").subtract(stream.select("id")).count()
    only_in_stream = stream.select("id").subtract(batch.select("id")).count()
    print(f"ids only in batch:    {only_in_batch}")
    print(f"ids only in streaming:{only_in_stream}")

    passed = (
        pct_delta < args.threshold_pct and only_in_batch == 0 and only_in_stream == 0
    )
    print(f"\n[reconcile] pct < {args.threshold_pct}% AND no orphan ids: {passed}")
    spark.stop()
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
