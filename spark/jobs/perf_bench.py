"""Sprint 5b performance benchmark for the Bronze → Silver path.

Hypothesis (ADR-0003 + ADR-0007 work):
    Bronze is partitioned by ingest_hour. Each Silver model filters
    by `type` (PushEvent, PullRequestEvent, …). Since `type` is
    independent of ingest_hour, the filter currently scans every
    file in every partition.

    Running `OPTIMIZE ... ZORDER BY (type)` on Bronze should cluster
    rows of the same type into the same files, letting Delta's
    data-skipping prune most of the read on a per-type filter. We
    expect:
        - lower Bronze read bytes per Silver build
        - lower wall-clock per Silver build
        - some up-front cost (OPTIMIZE compacts files; storage may
          go up due to re-written file headers)

This benchmark captures 5 dimensions per Silver model:

    1. Bronze read bytes (driver-side, via Spark Listener)
    2. Wall-clock seconds (from start of dbt run to end)
    3. Output rows
    4. Output file count
    5. Output bytes

It runs the benchmark twice (before / after ZORDER), then prints a
diff table for the report.

Usage:
    uv run python -m spark.jobs.perf_bench
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_PATH = "data/bronze/events"
SILVER_WAREHOUSE = "dbt/spark-warehouse/silver.db"
JAVA_HOME = "/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home"

SILVER_MODELS = [
    ("events_push", "PushEvent"),
    ("events_pull_request", "PullRequestEvent"),
    ("events_issue_comment", "IssueCommentEvent"),
    ("events_issues", "IssuesEvent"),
    ("events_watch", "WatchEvent"),
    ("events_fork", "ForkEvent"),
]


def build_spark(app: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "4g")
        .config("spark.databricks.delta.optimize.maxFileSize", "134217728")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def dir_bytes(path: str) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def parquet_file_count(path: str) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    return sum(1 for f in p.rglob("*.parquet"))


def bronze_filter_metrics(spark: SparkSession, event_type: str) -> dict:
    """Measure the cost of selecting one event type from Bronze.

    Returns: {read_files, read_bytes, output_rows, wall_seconds}
    """
    df = (
        spark.read.format("delta").load(BRONZE_PATH).filter(F.col("type") == event_type)
    )
    t0 = time.perf_counter()
    rows = df.count()
    wall = time.perf_counter() - t0

    # Approximate read footprint: data-skipping rewrites are visible in
    # `inputFiles()` which counts the post-prune file list.
    files_read = len(df.inputFiles())
    # Bronze total bytes (denominator for any pruning ratio in the report).
    bronze_total = dir_bytes(BRONZE_PATH)
    return {
        "wall_seconds": round(wall, 2),
        "rows": rows,
        "bronze_files_after_prune": files_read,
        "bronze_total_bytes": bronze_total,
    }


def silver_state(model_name: str) -> dict:
    path = f"{SILVER_WAREHOUSE}/{model_name}"
    return {
        "output_files": parquet_file_count(path),
        "output_bytes": dir_bytes(path),
    }


def run_dbt_silver_build(full_refresh: bool = True) -> float:
    """Force-rebuild all 6 Silver models, return wall-clock seconds."""
    cmd = ["../.venv/bin/dbt", "run", "--select", "silver"]
    if full_refresh:
        cmd.append("--full-refresh")
    t0 = time.perf_counter()
    subprocess.run(
        cmd,
        cwd="dbt",
        env={
            "JAVA_HOME": JAVA_HOME,
            "PATH": f"{JAVA_HOME}/bin:" + __import__("os").environ.get("PATH", ""),
            "PYSPARK_SUBMIT_ARGS": "--driver-memory 4g pyspark-shell",
            "HOME": __import__("os").environ.get("HOME", ""),
        },
        check=True,
        capture_output=True,
    )
    return round(time.perf_counter() - t0, 2)


def measure_round(spark: SparkSession, label: str) -> dict:
    print(f"\n[{label}] rebuilding all 6 Silver models from Bronze ...")
    silver_wall = run_dbt_silver_build(full_refresh=True)
    print(f"[{label}] silver build wall clock: {silver_wall}s")

    per_type: dict[str, dict] = {}
    for model_name, event_type in SILVER_MODELS:
        filter_m = bronze_filter_metrics(spark, event_type)
        out = silver_state(model_name)
        per_type[model_name] = {**filter_m, **out}
        print(
            f"  {model_name:<25} filter→{filter_m['wall_seconds']}s, "
            f"files_read={filter_m['bronze_files_after_prune']}, "
            f"out_files={out['output_files']}, "
            f"out_bytes={out['output_bytes']:,}"
        )

    return {
        "label": label,
        "silver_full_refresh_seconds": silver_wall,
        "bronze_total_bytes": dir_bytes(BRONZE_PATH),
        "bronze_total_files": parquet_file_count(BRONZE_PATH),
        "per_silver_model": per_type,
    }


def run_optimize_zorder(spark: SparkSession) -> dict:
    print("\n[OPTIMIZE] running OPTIMIZE ... ZORDER BY (type) on Bronze ...")
    bronze_before_bytes = dir_bytes(BRONZE_PATH)
    bronze_before_files = parquet_file_count(BRONZE_PATH)
    t0 = time.perf_counter()
    DeltaTable.forPath(spark, BRONZE_PATH).optimize().executeZOrderBy("type")
    wall = round(time.perf_counter() - t0, 2)
    return {
        "wall_seconds": wall,
        "bronze_bytes_before": bronze_before_bytes,
        "bronze_files_before": bronze_before_files,
        "bronze_bytes_after": dir_bytes(BRONZE_PATH),
        "bronze_files_after": parquet_file_count(BRONZE_PATH),
    }


def main() -> None:
    spark = build_spark("perf_bench")
    spark.sparkContext.setLogLevel("ERROR")

    before = measure_round(spark, "BEFORE")
    opt = run_optimize_zorder(spark)
    print(f"[OPTIMIZE] done in {opt['wall_seconds']}s")
    print(
        f"[OPTIMIZE] bronze files: {opt['bronze_files_before']} → "
        f"{opt['bronze_files_after']}; "
        f"bytes: {opt['bronze_bytes_before']:,} → {opt['bronze_bytes_after']:,}"
    )
    after = measure_round(spark, "AFTER")

    report = {
        "before": before,
        "optimize": opt,
        "after": after,
    }
    out_path = Path("docs/performance/perf_bench.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    print(f"\n[summary] wrote {out_path}")
    print(
        f"\nbefore silver build: {before['silver_full_refresh_seconds']}s"
        f"\nafter  silver build: {after['silver_full_refresh_seconds']}s"
    )

    spark.stop()


if __name__ == "__main__":
    main()
