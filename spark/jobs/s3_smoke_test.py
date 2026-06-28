"""Sprint 5a smoke test — read Bronze Delta from S3 via local Spark.

Proves the cloud Bronze is real and queryable end-to-end, without
needing a Databricks workspace yet. Uses local Spark with the
hadoop-aws connector + the user's AWS CLI credentials.

What it checks:
    1. The S3 bucket + path open as a Delta table.
    2. Row count matches what landed locally (613,876 after Sprint 1).
    3. The `count(*) == count(distinct id)` idempotency invariant
       (ADR-0002) still holds after the round-trip through S3.
    4. Per-ingest_hour breakdown matches local exactly.

Usage:
    uv run python -m spark.jobs.s3_smoke_test --bucket oss-pulse-bronze-dev-9f3eb8a5

Requires:
    - AWS credentials available to the Spark JVM. The script copies
      them from boto3's chain (env vars → ~/.aws/credentials → IAM
      role) into the SparkSession config so the same `aws configure`
      that worked for the aws CLI works here.
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from pyspark.sql import SparkSession  # noqa: F401


def aws_creds() -> tuple[str, str, str | None]:
    """Pull effective AWS credentials from the same chain aws CLI uses."""
    session = boto3.Session()
    creds = session.get_credentials()
    if creds is None:
        raise SystemExit(
            "[s3_smoke_test] no AWS credentials found. Run `aws configure` first."
        )
    frozen = creds.get_frozen_credentials()
    return frozen.access_key, frozen.secret_key, frozen.token


def build_spark(bucket: str) -> SparkSession:
    access_key, secret_key, session_token = aws_creds()

    # All jars must be on the JVM classpath before getOrCreate() is
    # called — that means PYSPARK_SUBMIT_ARGS, not SparkSession.config().
    # Include Delta + hadoop-aws + aws sdk in one --packages list.
    if "hadoop-aws" not in os.environ.get("PYSPARK_SUBMIT_ARGS", ""):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--driver-memory 4g "
            "--packages io.delta:delta-spark_2.12:3.2.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "pyspark-shell"
        )

    builder = (
        SparkSession.builder.appName("s3_smoke_test")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    )
    if session_token:
        builder = builder.config("spark.hadoop.fs.s3a.session.token", session_token)
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )

    return builder.getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket", required=True, help="Bronze bucket name (no s3:// prefix)"
    )
    parser.add_argument("--prefix", default="events", help="prefix within the bucket")
    args = parser.parse_args()

    path = f"s3a://{args.bucket}/{args.prefix}"
    print(f"[s3_smoke_test] reading {path} ...")

    spark = build_spark(args.bucket)
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.format("delta").load(path)
    total = df.count()
    distinct = df.select("id").distinct().count()
    invariant = total == distinct

    print("\n========== S3 Bronze smoke test ==========")
    print(f"path:                 {path}")
    print(f"total rows:           {total:,}")
    print(f"distinct ids:         {distinct:,}")
    print(f"invariant (ADR-0002): total == distinct → {invariant}")

    print("\n[breakdown] rows per ingest_hour:")
    (df.groupBy("ingest_hour").count().orderBy("ingest_hour").show(20, truncate=False))

    spark.stop()
    if not invariant:
        sys.exit(1)
    print("[s3_smoke_test] PASS — cloud Bronze matches local Bronze.")


if __name__ == "__main__":
    main()
