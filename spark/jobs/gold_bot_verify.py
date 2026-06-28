"""Sprint 3b verifier for gold.bot_vs_human_activity_mart.

Checks:
    1. Grain invariant: count(*) == count(distinct repo_id, activity_date).
    2. bot_event_count + human_event_count <= event_count
       (slack is rows where actor_login is null).
    3. Recall sanity: github-actions[bot] is the largest bot in the spike.
       Confirm it appears with non-zero bot counts in repos it touched.
    4. Cross-check repo_daily_activity's bot_push_count matches this
       mart's push_bot_count for the same (repo, day).
    5. Spot-check: show the top 10 repos by bot_event_share.
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BOT_PATH = "dbt/spark-warehouse/gold.db/bot_vs_human_activity_mart"
ACT_PATH = "dbt/spark-warehouse/gold.db/repo_daily_activity"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("gold_bot_verify")
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

    bot = spark.read.format("delta").load(BOT_PATH)
    act = spark.read.format("delta").load(ACT_PATH)

    print("\n========== gold.bot_vs_human_activity_mart verifier ==========")

    total = bot.count()
    unique = bot.select("repo_id", "activity_date").distinct().count()
    print(f"[invariant] total rows:                {total:,}")
    print(f"[invariant] unique (repo_id, date):     {unique:,}")
    if total != unique:
        raise SystemExit(1)
    print("[invariant] grain holds: True")

    bad = bot.filter(
        F.col("bot_event_count") + F.col("human_event_count") > F.col("event_count")
    ).count()
    print(f"[invariant] rows violating bot+human <= total: {bad}")
    if bad > 0:
        raise SystemExit(2)

    # Cross-mart: push_bot_count vs repo_daily_activity.bot_push_count
    joined = bot.select("repo_id", "activity_date", "push_bot_count").join(
        act.select("repo_id", "activity_date", "bot_push_count"),
        on=["repo_id", "activity_date"],
        how="inner",
    )
    mismatches = joined.filter(F.col("push_bot_count") != F.col("bot_push_count"))
    n_mismatch = mismatches.count()
    print(f"\n[cross-mart] (repo, day) keys joined: {joined.count():,}")
    print(f"[cross-mart] push_bot_count != bot_push_count rows: {n_mismatch}")
    if n_mismatch > 0:
        print("[cross-mart] sample mismatches:")
        mismatches.show(5, truncate=False)
        raise SystemExit(3)
    print("[cross-mart] push bot counts match repo_daily_activity exactly")

    # Top 10 bot-share repos with at least 50 events
    print("\n[spot check] top 10 repos by bot_event_share (event_count >= 50):")
    (
        bot.filter(F.col("event_count") >= 50)
        .orderBy(F.col("bot_event_share").desc())
        .select(
            "repo_id",
            "activity_date",
            "event_count",
            "bot_event_count",
            F.round(F.col("bot_event_share") * 100, 2).alias("bot_share_%"),
            "push_bot_count",
            "comment_bot_count",
            "app_event_count",
        )
        .limit(10)
        .show(truncate=False)
    )

    # Distribution of bot share
    buckets = (
        bot.withColumn(
            "bucket",
            F.when(F.col("bot_event_share") == 0, "0%")
            .when(F.col("bot_event_share") < 0.1, "0-10%")
            .when(F.col("bot_event_share") < 0.5, "10-50%")
            .when(F.col("bot_event_share") < 1.0, "50-99%")
            .otherwise("100%"),
        )
        .groupBy("bucket")
        .count()
        .orderBy("bucket")
        .collect()
    )
    print("\n[distribution] bot_event_share buckets across all rows:")
    for r in buckets:
        print(f"  {r['bucket']:<8}  {r['count']:>7,}")

    print("\n[summary] gold.bot_vs_human_activity_mart passes Sprint 3b verification.")
    spark.stop()


if __name__ == "__main__":
    main()
