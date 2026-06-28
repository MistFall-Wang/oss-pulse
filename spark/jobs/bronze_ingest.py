"""Bronze ingestion job: GH Archive hourly JSON to Bronze Delta table.

Usage:
    uv run python -m spark.jobs.bronze_ingest \
        --source data/raw/2025-01-15-12.json.gz \
        --bronze-path data/bronze/events

Idempotency contract:
    Running this script N times on the same source file produces the same row
    count and the same set of event ids.
"""

from __future__ import annotations

import argparse
import re
import uuid
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.schemas import BRONZE_EVENTS_SCHEMA

_HOUR_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}-\d{1,2})\.json\.gz$")


def build_spark(app_name: str = "bronze_ingest") -> SparkSession:
    """Build a local SparkSession with Delta Lake enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def extract_ingest_hour(source_path: str) -> str:
    """Pull the YYYY-MM-DD-HH stamp out of the GH Archive filename."""
    name = Path(source_path).name
    match = _HOUR_PATTERN.search(name)
    if not match:
        raise ValueError(f"cannot extract ingest_hour from {name!r}")
    return match.group(1)


def read_raw_events(spark: SparkSession, source_path: str) -> DataFrame:
    """Read a single hourly GH Archive JSON.gz file as raw text."""
    return (
        spark.read.option("compression", "gzip")
        .text(source_path)
        .withColumnRenamed("value", "raw_line")
    )


def shape_to_bronze(
    raw: DataFrame,
    source_file: str,
    ingest_hour: str,
    ingest_run_id: str,
) -> DataFrame:
    """Project raw text rows into the Bronze envelope schema."""
    df = (
        raw.select(
            F.get_json_object("raw_line", "$.id").alias("id"),
            F.get_json_object("raw_line", "$.type").alias("type"),
            F.get_json_object("raw_line", "$.actor.id").cast("long").alias("actor_id"),
            F.get_json_object("raw_line", "$.actor.login").alias("actor_login"),
            F.get_json_object("raw_line", "$.repo.id").cast("long").alias("repo_id"),
            F.get_json_object("raw_line", "$.repo.name").alias("repo_name"),
            F.get_json_object("raw_line", "$.org.id").cast("long").alias("org_id"),
            F.get_json_object("raw_line", "$.org.login").alias("org_login"),
            F.get_json_object("raw_line", "$.public")
            .cast("boolean")
            .alias("is_public"),
            F.get_json_object("raw_line", "$.created_at").alias("created_at_raw"),
            F.get_json_object("raw_line", "$.payload").alias("payload_raw"),
        )
        .withColumn(
            "created_at",
            F.to_timestamp("created_at_raw", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingest_hour", F.lit(ingest_hour))
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
    )

    column_order = [field.name for field in BRONZE_EVENTS_SCHEMA.fields]
    return df.select(*column_order)


def write_bronze(spark: SparkSession, batch: DataFrame, bronze_path: str) -> None:
    """Merge one ingestion batch into the Bronze Delta table."""
    table_path = Path(bronze_path)
    if not (table_path / "_delta_log").exists():
        batch.write.format("delta").partitionBy("ingest_hour").mode("overwrite").save(
            bronze_path
        )
        return

    target = DeltaTable.forPath(spark, bronze_path)
    (
        target.alias("t")
        .merge(batch.alias("s"), "t.id = s.id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="path to .json.gz")
    parser.add_argument(
        "--bronze-path",
        default="data/bronze/events",
        help="path to Bronze Delta table",
    )
    args = parser.parse_args()

    ingest_hour = extract_ingest_hour(args.source)
    ingest_run_id = str(uuid.uuid4())
    source_file = str(Path(args.source).resolve())

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_raw_events(spark, args.source)
    batch = shape_to_bronze(
        raw,
        source_file=source_file,
        ingest_hour=ingest_hour,
        ingest_run_id=ingest_run_id,
    )

    in_count = batch.count()
    print(f"\n[ingest] source={args.source}")
    print(f"[ingest] ingest_hour={ingest_hour}")
    print(f"[ingest] ingest_run_id={ingest_run_id}")
    print(f"[ingest] events in batch: {in_count:,}")

    write_bronze(spark, batch, args.bronze_path)

    bronze = spark.read.format("delta").load(args.bronze_path)
    total = bronze.count()
    unique = bronze.select("id").distinct().count()
    print(f"\n[verify] total bronze rows:   {total:,}")
    print(f"[verify] unique bronze ids:   {unique:,}")
    print(f"[verify] invariant (total == unique): {total == unique}")

    spark.stop()


if __name__ == "__main__":
    main()
