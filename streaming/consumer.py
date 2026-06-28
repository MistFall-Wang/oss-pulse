"""Structured Streaming consumer that reads gh-events from Redpanda,
parses the PushEvent envelope + payload, and MERGEs into a parallel
Delta table `silver_streaming.events_push`.

Sprint 6 MVP. Design notes:

- **Idempotency**: `foreachBatch` + Delta MERGE on `id` mirrors the
  batch Bronze contract (ADR-0002). Replaying the same hour to Kafka
  twice produces zero duplicate rows downstream.
- **Late events**: tolerated implicitly by MERGE — a late row with the
  same id just no-ops. No watermark in the MVP; Sprint 7-9 would add
  watermark-based dropping for grossly-late events plus state cleanup.
- **Schema-drift containment**: same as batch — Kafka message body is
  the raw event JSON STRING, parsed once per micro-batch. The
  streaming table has the same shape as `silver.events_push` so a
  reconciliation can do a simple `EXCEPT` / count compare.
- **Exactly-once write**: Delta + foreachBatch achieves it by virtue
  of MERGE's natural idempotency; we don't need additional
  transactional offsets.

The consumer is a one-shot trigger (`availableNow=True`): it drains
the topic and exits, which is the right shape for a demo / batch
backfill. A continuous run would use `trigger(processingTime='10s')`
instead.

Usage:
    uv run python -m streaming.consumer \\
        --bootstrap localhost:19094 \\
        --topic gh-events \\
        --table-path data/streaming/silver_streaming/events_push \\
        --checkpoint-path data/streaming/_checkpoints/events_push
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# Delta + Kafka jars must be on the JVM classpath BEFORE the gateway
# launches. SparkSession.builder.config('spark.jars.packages', ...) is
# too late — set PYSPARK_SUBMIT_ARGS pre-getOrCreate.
# Note: spark-sql-kafka-0-10_2.12:3.5.3 isn't published to Maven Central
# even though PySpark 3.5.3 is. Use 3.5.8 (latest 3.5.x patch with the
# kafka connector published) — binary-compatible with PySpark 3.5.3.
DEPS = (
    "io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
)


def build_spark() -> SparkSession:
    if "--packages" not in os.environ.get("PYSPARK_SUBMIT_ARGS", ""):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--driver-memory 4g --packages {DEPS} pyspark-shell"
        )
    return (
        SparkSession.builder.appName("oss_pulse_streaming_consumer")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def parse_envelope(events: DataFrame) -> DataFrame:
    """Mirror spark/jobs/bronze_ingest.py's shape_to_bronze step plus
    silver/events_push.sql's payload parse — but in one pass since
    we're reading from Kafka, not Bronze.
    """
    return events.select(
        F.get_json_object("raw_line", "$.id").alias("id"),
        F.get_json_object("raw_line", "$.actor.id").cast("long").alias("actor_id"),
        F.get_json_object("raw_line", "$.actor.login").alias("actor_login"),
        F.get_json_object("raw_line", "$.repo.id").cast("long").alias("repo_id"),
        F.get_json_object("raw_line", "$.repo.name").alias("repo_name"),
        F.get_json_object("raw_line", "$.org.id").cast("long").alias("org_id"),
        F.get_json_object("raw_line", "$.org.login").alias("org_login"),
        F.get_json_object("raw_line", "$.public").cast("boolean").alias("is_public"),
        F.to_timestamp(
            F.get_json_object("raw_line", "$.created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"
        ).alias("created_at"),
        F.coalesce(
            F.get_json_object("raw_line", "$.payload.size").cast("int"),
            F.get_json_object("raw_line", "$.payload.commit_count").cast("int"),
        ).alias("commit_size"),
        F.get_json_object("raw_line", "$.payload.push_id")
        .cast("long")
        .alias("push_id"),
        F.get_json_object("raw_line", "$.payload.ref").alias("ref"),
    )


def make_writer(spark: SparkSession, table_path: str):
    """Returns a foreachBatch handler that MERGEs each micro-batch into
    the streaming silver table.
    """

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_count = batch_df.count()
        if batch_count == 0:
            return
        # Initialize table on first non-empty batch.
        if not (Path(table_path) / "_delta_log").exists():
            batch_df.write.format("delta").mode("overwrite").save(table_path)
            print(
                f"[consumer] batch {batch_id}: bootstrapped table with {batch_count:,} rows"
            )
            return
        target = DeltaTable.forPath(spark, table_path)
        (
            target.alias("t")
            .merge(batch_df.alias("s"), "t.id = s.id")
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[consumer] batch {batch_id}: merged {batch_count:,} rows")

    return write_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:19094")
    parser.add_argument("--topic", default="gh-events")
    parser.add_argument(
        "--table-path", default="data/streaming/silver_streaming/events_push"
    )
    parser.add_argument(
        "--checkpoint-path", default="data/streaming/_checkpoints/events_push"
    )
    args = parser.parse_args()

    Path(args.table_path).mkdir(parents=True, exist_ok=True)
    Path(args.checkpoint_path).mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_line")
    )
    parsed = parse_envelope(kafka_df).filter(F.col("id").isNotNull())

    query = (
        parsed.writeStream.foreachBatch(make_writer(spark, args.table_path))
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    print(f"[consumer] streaming from {args.topic} → {args.table_path} (availableNow)")
    query.awaitTermination()
    print("[consumer] drained. stopping.")
    spark.stop()


if __name__ == "__main__":
    main()
