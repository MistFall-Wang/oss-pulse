"""CLI to run a quality suite for a given layer.

Exits non-zero on any failed check, so Airflow can gate downstream
tasks with a simple BashOperator (no special sensor needed).

Usage:
    uv run python -m quality.runner --layer bronze
    uv run python -m quality.runner --layer silver
    uv run python -m quality.runner --layer gold
    uv run python -m quality.runner --layer cross_mart
"""

from __future__ import annotations

import argparse
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from quality import checks


BRONZE_PATH = "data/bronze/events"
SILVER_WAREHOUSE = "dbt/spark-warehouse/silver.db"
GOLD_WAREHOUSE = "dbt/spark-warehouse/gold.db"


def build_spark() -> SparkSession:
    # Drive heap via PYSPARK_SUBMIT_ARGS so the JVM is launched with
    # enough headroom for distinct() / aggregate() on the Bronze table.
    # spark.driver.memory in the builder is ignored for local[*] once the
    # JVM is up; the env var is the only reliable lever in session mode.
    import os
    args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if "--driver-memory" not in args:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--driver-memory 4g " + args + " pyspark-shell"
        ).strip()
    builder = (
        SparkSession.builder.appName("quality_runner")
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
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_bronze(spark: SparkSession) -> list[checks.CheckResult]:
    bronze = spark.read.format("delta").load(BRONZE_PATH).select(
        "id", "type", "created_at", "is_public"
    )
    # Selecting only the columns we need keeps payload_raw out of the
    # driver heap on distinct/aggregate operations.
    return [
        checks.bronze_id_unique_and_not_null(bronze),
        checks.bronze_type_in_known_set(bronze),
        checks.bronze_is_public_always_true(bronze),
        checks.bronze_created_at_not_null(bronze),
    ]


def run_silver(spark: SparkSession) -> list[checks.CheckResult]:
    bronze = spark.read.format("delta").load(BRONZE_PATH).select("type")
    push = spark.read.format("delta").load(f"{SILVER_WAREHOUSE}/events_push")
    pr = spark.read.format("delta").load(f"{SILVER_WAREHOUSE}/events_pull_request")
    issues = spark.read.format("delta").load(f"{SILVER_WAREHOUSE}/events_issues")
    comments = spark.read.format("delta").load(f"{SILVER_WAREHOUSE}/events_issue_comment")
    watch = spark.read.format("delta").load(f"{SILVER_WAREHOUSE}/events_watch")
    fork = spark.read.format("delta").load(f"{SILVER_WAREHOUSE}/events_fork")
    results = [
        checks.silver_row_count_matches_bronze(push,     bronze, "PushEvent",         "events_push"),
        checks.silver_row_count_matches_bronze(pr,       bronze, "PullRequestEvent",  "events_pull_request"),
        checks.silver_row_count_matches_bronze(issues,   bronze, "IssuesEvent",       "events_issues"),
        checks.silver_row_count_matches_bronze(comments, bronze, "IssueCommentEvent", "events_issue_comment"),
        checks.silver_row_count_matches_bronze(watch,    bronze, "WatchEvent",        "events_watch"),
        checks.silver_row_count_matches_bronze(fork,     bronze, "ForkEvent",         "events_fork"),
        checks.silver_pr_state_in_known(pr),
    ]
    bronze.unpersist()
    return results


def run_gold(spark: SparkSession) -> list[checks.CheckResult]:
    activity = spark.read.format("delta").load(f"{GOLD_WAREHOUSE}/repo_daily_activity")
    health = spark.read.format("delta").load(f"{GOLD_WAREHOUSE}/oss_health_mart")
    bot = spark.read.format("delta").load(f"{GOLD_WAREHOUSE}/bot_vs_human_activity_mart")
    return [
        checks.gold_grain_unique(activity, ("repo_id", "activity_date"), "repo_daily_activity"),
        checks.gold_grain_unique(health,   ("repo_id", "activity_date"), "oss_health_mart"),
        checks.gold_grain_unique(bot,      ("repo_id", "activity_date"), "bot_vs_human_activity_mart"),
        checks.gold_health_pr_merged_le_closed(health),
        checks.gold_bot_share_in_range(bot),
    ]


def run_cross_mart(spark: SparkSession) -> list[checks.CheckResult]:
    activity = spark.read.format("delta").load(f"{GOLD_WAREHOUSE}/repo_daily_activity")
    bot = spark.read.format("delta").load(f"{GOLD_WAREHOUSE}/bot_vs_human_activity_mart")
    return [checks.cross_mart_bot_push_count_agrees(activity, bot)]


SUITES = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
    "cross_mart": run_cross_mart,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=sorted(SUITES.keys()))
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n========== quality suite: {args.layer} ==========")
    results = SUITES[args.layer](spark)
    for r in results:
        print(r)

    failed = [r for r in results if not r.passed]
    print(
        f"\n[summary] {len(results) - len(failed)} passed, {len(failed)} failed"
    )

    spark.stop()
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
