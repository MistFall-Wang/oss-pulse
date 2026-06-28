"""Sprint 3a step 6 verifier for gold.oss_health_mart.

Runs three checks:

    1. Grain invariant: count(*) == count(distinct repo_id, activity_date).
       Same belt-and-suspenders pattern as gold_verify.py for
       repo_daily_activity.

    2. PR-merge ground-truth cross-check: pick the busiest
       (repo_id, activity_date) with merged PRs in the mart, recompute
       pr_merged_count and pr_avg_merge_latency_hours straight from
       silver.events_pull_request, and assert they match.

    3. Cross-mart contributor coverage: for the same key, verify that
       unique_contributors >= unique_pushers from repo_daily_activity
       on that day is NOT necessarily true (push-only days can have more
       pushers than the cross-event contributors here, since this mart
       doesn't include PushEvent). Just print both for eyeballing.

Read-only. Does not touch any Delta table.
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HEALTH_PATH = "dbt/spark-warehouse/gold.db/oss_health_mart"
ACTIVITY_PATH = "dbt/spark-warehouse/gold.db/repo_daily_activity"
SILVER_PR_PATH = "dbt/spark-warehouse/silver.db/events_pull_request"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("gold_health_verify")
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

    health = spark.read.format("delta").load(HEALTH_PATH)
    activity = spark.read.format("delta").load(ACTIVITY_PATH)
    silver_pr = spark.read.format("delta").load(SILVER_PR_PATH)

    print("\n========== gold.oss_health_mart verifier ==========")
    total = health.count()
    unique = health.select("repo_id", "activity_date").distinct().count()
    print(f"[invariant] total rows:                {total:,}")
    print(f"[invariant] unique (repo_id, date):     {unique:,}")
    ok = total == unique
    print(f"[invariant] grain holds (total == unique): {ok}")
    if not ok:
        raise SystemExit(1)

    busiest = (
        health.filter("pr_merged_count > 0")
        .orderBy(F.col("pr_merged_count").desc())
        .limit(1)
        .collect()
    )
    if not busiest:
        print("[ground truth] no merged PRs in sample, skipping merge-latency check")
    else:
        row = busiest[0]
        repo_id = row["repo_id"]
        activity_date = row["activity_date"]
        mart_merged = row["pr_merged_count"]
        mart_latency = row["pr_avg_merge_latency_hours"]

        print(
            "\n[ground truth] busiest merged-PR row in mart:\n"
            f"    repo_id                     = {repo_id}\n"
            f"    activity_date               = {activity_date}\n"
            f"    pr_merged_count             = {mart_merged}\n"
            f"    pr_avg_merge_latency_hours  = "
            f"{mart_latency:.3f}" if mart_latency is not None else "    pr_avg_merge_latency_hours  = NULL"
        )

        recomputed = silver_pr.filter(
            (F.col("repo_id") == repo_id)
            & (F.to_date("created_at") == F.lit(activity_date))
            & (F.col("action") == "closed")
            & (F.col("pr_merged") == True)  # noqa: E712
        ).agg(
            F.count("*").alias("merged_count"),
            (
                F.avg(
                    (F.unix_timestamp("pr_merged_at") - F.unix_timestamp("pr_created_at"))
                    / 3600.0
                )
            ).alias("avg_latency_hours"),
        ).collect()[0]

        print(
            "\n[ground truth] silver recomputation:\n"
            f"    merged_count                = {recomputed['merged_count']}\n"
            f"    avg_latency_hours           = "
            + (
                f"{recomputed['avg_latency_hours']:.3f}"
                if recomputed["avg_latency_hours"] is not None
                else "NULL"
            )
        )

        count_ok = mart_merged == recomputed["merged_count"]
        mart_latency_f = float(mart_latency) if mart_latency is not None else None
        recomp_latency = recomputed["avg_latency_hours"]
        recomp_latency_f = float(recomp_latency) if recomp_latency is not None else None
        if mart_latency_f is None and recomp_latency_f is None:
            latency_ok = True
        elif mart_latency_f is None or recomp_latency_f is None:
            latency_ok = False
        else:
            latency_ok = abs(mart_latency_f - recomp_latency_f) < 1e-3

        print(
            f"\n[ground truth] merged_count match:  {count_ok}\n"
            f"[ground truth] latency match (1ms):  {latency_ok}"
        )
        if not (count_ok and latency_ok):
            raise SystemExit(2)

        joined = (
            health.filter(
                (F.col("repo_id") == repo_id) & (F.col("activity_date") == activity_date)
            )
            .select("repo_id", "activity_date", "unique_contributors")
            .join(
                activity.select("repo_id", "activity_date", "unique_pushers"),
                on=["repo_id", "activity_date"],
                how="left",
            )
            .collect()
        )
        if joined:
            j = joined[0]
            print(
                "\n[cross-mart] unique_contributors (PR+issue+comment) vs unique_pushers:\n"
                f"    unique_contributors = {j['unique_contributors']}\n"
                f"    unique_pushers      = {j['unique_pushers']}\n"
                "    (no required ordering — they measure different actor sets)"
            )

    print("\n[summary] gold.oss_health_mart passes Sprint 3a verification.")
    spark.stop()


if __name__ == "__main__":
    main()
