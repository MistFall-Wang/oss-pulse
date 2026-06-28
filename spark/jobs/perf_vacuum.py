"""Companion to perf_bench: VACUUM Bronze and measure post-cleanup state.

OPTIMIZE writes new compacted files but does NOT delete the source
files — they're orphaned, waiting for VACUUM. This script:

    1. Records the pre-VACUUM Bronze footprint
    2. VACUUMs Bronze (with retention=0h, only safe in dev — see ADR-0009)
    3. Records the post-VACUUM Bronze footprint

The output goes to docs/performance/perf_vacuum.json. The
docs/performance/sprint5b_tuning.md report uses both this and
perf_bench.json.
"""

from __future__ import annotations

import json
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession


BRONZE_PATH = "data/bronze/events"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("perf_vacuum")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # CRITICAL: only set this to false in dev. In prod, Delta refuses
        # retention < 7 days because anyone reading the table during
        # vacuum could see corrupt rows. ADR-0009 will codify the
        # production cadence (compact daily, vacuum weekly with 168h
        # retention).
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "4g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def dir_bytes(path: str) -> int:
    p = Path(path)
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def parquet_file_count(path: str) -> int:
    return sum(1 for _ in Path(path).rglob("*.parquet"))


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    before = {
        "bytes": dir_bytes(BRONZE_PATH),
        "files": parquet_file_count(BRONZE_PATH),
    }
    print(f"[before] bytes={before['bytes']:,} files={before['files']}")

    print("[vacuum] running VACUUM ... RETAIN 0 HOURS on Bronze ...")
    DeltaTable.forPath(spark, BRONZE_PATH).vacuum(retentionHours=0)

    after = {
        "bytes": dir_bytes(BRONZE_PATH),
        "files": parquet_file_count(BRONZE_PATH),
    }
    print(f"[after]  bytes={after['bytes']:,} files={after['files']}")

    Path("docs/performance/perf_vacuum.json").write_text(
        json.dumps({"before": before, "after": after}, indent=2)
    )
    spark.stop()


if __name__ == "__main__":
    main()
