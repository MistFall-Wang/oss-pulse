"""Sprint 5b deliberate incident: rename payload.size to payload.commit_count
in a synthetic GH Archive hour and ingest it as a new ingest_hour.

Goal: observe exactly where the schema break is detected — and where
it isn't — so the postmortem can specify which gate failed first and
which gates leaked.

Outputs:
    /tmp/incident_2099-12-31-23.json.gz — synthetic source file
    data/bronze/events (ingest_hour=2099-12-31-23 added) — Bronze write
    /tmp/incident_observation.json — what each check returned

This script is reversible: a separate cleanup function deletes the
synthetic partition from Bronze. Re-running the rest of the pipeline
afterwards returns to a clean state.
"""

from __future__ import annotations

import gzip
import json
import shutil
import time
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession


INCIDENT_HOUR = "2099-12-31-23"
INCIDENT_FILE = f"/tmp/incident_{INCIDENT_HOUR}.json.gz"
BRONZE_PATH = "data/bronze/events"
SOURCE_TEMPLATE = "data/raw/2025-01-15-12.json.gz"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("incident_inject")
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


def synthesize_incident_file(n_rows: int = 200) -> None:
    """Take the first n_rows PushEvents from the 2025-01-15-12 source
    and rename payload.size → payload.commit_count. Also rewrite the
    event id and ingest hour so we don't collide with existing Bronze
    rows. Write to /tmp/incident_<hour>.json.gz.
    """
    out_lines: list[str] = []
    seen = 0
    base_id = 999_000_000_000  # synthetic id space well above real GitHub ids
    with gzip.open(SOURCE_TEMPLATE, "rt") as src:
        for line in src:
            event = json.loads(line)
            if event.get("type") != "PushEvent":
                continue
            payload = event["payload"]
            if "size" not in payload:
                continue
            # The breaking change: rename payload.size → payload.commit_count
            payload["commit_count"] = payload.pop("size")
            event["id"] = str(base_id + seen)
            event["created_at"] = "2099-12-31T23:00:00Z"
            out_lines.append(json.dumps(event))
            seen += 1
            if seen >= n_rows:
                break

    with gzip.open(INCIDENT_FILE, "wt") as out:
        for line in out_lines:
            out.write(line + "\n")
    print(f"[inject] wrote {seen} broken PushEvent rows to {INCIDENT_FILE}")


def cleanup_incident(spark: SparkSession) -> None:
    """Remove the incident partition from Bronze and delete the
    synthetic source file."""
    print("[cleanup] removing incident partition from Bronze ...")
    table = DeltaTable.forPath(spark, BRONZE_PATH)
    table.delete(f"ingest_hour = '{INCIDENT_HOUR}'")
    incident = Path(INCIDENT_FILE)
    if incident.exists():
        incident.unlink()
    print("[cleanup] done.")


def main() -> None:
    import sys

    if "--cleanup" in sys.argv:
        spark = build_spark()
        cleanup_incident(spark)
        spark.stop()
        return

    synthesize_incident_file()

    print("[inject] ingesting incident file via the real Bronze job ...")
    import subprocess

    t0 = time.perf_counter()
    res = subprocess.run(
        [
            ".venv/bin/python",
            "-m",
            "spark.jobs.bronze_ingest",
            "--source",
            INCIDENT_FILE,
            "--bronze-path",
            BRONZE_PATH,
        ],
        capture_output=True,
        text=True,
        env={
            "JAVA_HOME": "/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home",
            "PATH": "/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home/bin:"
            + __import__("os").environ.get("PATH", ""),
            "HOME": __import__("os").environ.get("HOME", ""),
        },
    )
    print(f"[inject] bronze_ingest exit={res.returncode} in {time.perf_counter() - t0:.1f}s")
    if res.returncode != 0:
        print(res.stderr[-2000:])
        return

    # Show what landed in Bronze for the incident partition.
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    incident_rows = bronze.filter(f"ingest_hour = '{INCIDENT_HOUR}'")
    print(f"\n[bronze] incident partition row count: {incident_rows.count()}")
    print("[bronze] sample payload_raw key set on incident:")
    sample = incident_rows.select("payload_raw").limit(1).collect()[0]
    print("  ", sorted(json.loads(sample["payload_raw"]).keys()))
    print(
        "[bronze] notice: 'size' replaced by 'commit_count' "
        "(this is the schema break)"
    )
    spark.stop()
    print("\n[inject] now run:")
    print("  cd dbt && uv run dbt run --select silver.events_push")
    print("  uv run python -m quality.runner --layer silver")
    print("  cd dbt && uv run dbt test --select silver.events_push")
    print(
        "  then  uv run python -m spark.jobs.incident_inject --cleanup"
        "  to revert the project state."
    )


if __name__ == "__main__":
    main()
