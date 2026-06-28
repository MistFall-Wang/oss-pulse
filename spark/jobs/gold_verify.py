"""Sprint 2 step 4 verifier for gold.repo_daily_activity.

Runs the two checks the design doc requires:

    1. Grain invariant: count(*) == count(distinct repo_id, activity_date).
       Belt-and-suspenders next to the dbt_utils.unique_combination_of_columns
       schema test — proves the same property at runtime.

    2. Cross-layer ground truth: pick the busiest (repo_id, activity_date)
       in the mart and recompute push_count and total_commits straight
       from silver.events_push. They must match.

This script is read-only. It does not touch any Delta table.

Usage:
    uv run python -m spark.jobs.gold_verify
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_PATH = "dbt/spark-warehouse/gold.db/repo_daily_activity"
SILVER_PATH = "dbt/spark-warehouse/silver.db/events_push"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("gold_verify")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.host", "127.0.0.1")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    gold = spark.read.format("delta").load(GOLD_PATH)
    silver = spark.read.format("delta").load(SILVER_PATH)

    total = gold.count()
    unique_grain = (
        gold.select("repo_id", "activity_date").distinct().count()
    )

    print("\n========== gold.repo_daily_activity verifier ==========")
    print(f"gold path:        {GOLD_PATH}")
    print(f"silver path:      {SILVER_PATH}")
    print(f"\n[invariant] total rows:                {total:,}")
    print(f"[invariant] unique (repo_id, date) keys: {unique_grain:,}")
    invariant_ok = total == unique_grain
    print(f"[invariant] grain holds (total == unique): {invariant_ok}")
    if not invariant_ok:
        raise SystemExit(1)

    # Ground truth: take the busiest mart row, recompute it from silver.
    busiest = (
        gold.orderBy(F.col("push_count").desc())
        .select(
            "repo_id",
            "repo_name",
            "activity_date",
            "push_count",
            "total_commits",
            "unique_pushers",
            "bot_push_count",
        )
        .limit(1)
        .collect()[0]
    )

    repo_id = busiest["repo_id"]
    activity_date = busiest["activity_date"]
    print(
        "\n[ground truth] busiest mart row:\n"
        f"    repo_id        = {repo_id}\n"
        f"    repo_name      = {busiest['repo_name']}\n"
        f"    activity_date  = {activity_date}\n"
        f"    push_count     = {busiest['push_count']:,}\n"
        f"    total_commits  = {busiest['total_commits']:,}\n"
        f"    unique_pushers = {busiest['unique_pushers']:,}\n"
        f"    bot_push_count = {busiest['bot_push_count']:,}"
    )

    recomputed = silver.filter(
        (F.col("repo_id") == repo_id)
        & (F.to_date("created_at") == F.lit(activity_date))
    ).agg(
        F.count("*").alias("push_count"),
        F.sum("commit_size").alias("total_commits"),
        F.countDistinct("actor_id").alias("unique_pushers"),
        F.sum(
            F.when(F.col("actor_login").endswith("[bot]"), 1).otherwise(0)
        ).alias("bot_push_count"),
    ).collect()[0]

    print(
        "\n[ground truth] silver recomputed for same key:\n"
        f"    push_count     = {recomputed['push_count']:,}\n"
        f"    total_commits  = {recomputed['total_commits']:,}\n"
        f"    unique_pushers = {recomputed['unique_pushers']:,}\n"
        f"    bot_push_count = {recomputed['bot_push_count']:,}"
    )

    fields = ["push_count", "total_commits", "unique_pushers", "bot_push_count"]
    mismatches = [f for f in fields if busiest[f] != recomputed[f]]
    ok = not mismatches
    print(
        f"\n[ground truth] all four metrics match silver: {ok}"
        + (f"\n[ground truth] MISMATCH on: {mismatches}" if mismatches else "")
    )
    if not ok:
        raise SystemExit(2)

    print("\n[summary] gold.repo_daily_activity passes Sprint 2 step 4.")
    spark.stop()


if __name__ == "__main__":
    main()
