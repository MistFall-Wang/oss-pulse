"""Sprint 2.5 spike: validate proposed bot-identification heuristics.

We are about to codify ADR-0006 (bot identification rules) in Sprint 3.
Before committing to a rule, prove on real Bronze data that the union of
two heuristics catches most obviously-bot actors:

    Rule A: actor_login ends with '[bot]'
    Rule B: payload.performed_via_github_app is non-null

Outputs to stdout (saved into docs/spikes/bot_heuristic.md by the caller):
    1. Per-rule counts of distinct actors and events
    2. Overlap matrix (A only, B only, both, neither)
    3. Top 20 actors by event count, with rule_a / rule_b flags so we can
       eyeball coverage

Usage:
    uv run python -m spark.jobs.bot_heuristic_spike \
        --bronze-path data/bronze/events
"""

from __future__ import annotations

import argparse

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("bot_heuristic_spike")
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", default="data/bronze/events")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.format("delta").load(args.bronze_path)
    # Rule B as originally proposed checked payload.performed_via_github_app at
    # the root. Real GH Archive payloads put the field on sub-objects
    # (issue / comment / pull_request / review). Probe both shapes so the
    # spike documents the actual structure, not the assumed one.
    rule_b_paths = [
        "$.performed_via_github_app",
        "$.issue.performed_via_github_app",
        "$.comment.performed_via_github_app",
        "$.pull_request.performed_via_github_app",
        "$.review.performed_via_github_app",
    ]
    rule_b_expr = None
    for path in rule_b_paths:
        probe = F.get_json_object("payload_raw", path).isNotNull()
        rule_b_expr = probe if rule_b_expr is None else (rule_b_expr | probe)

    flagged = bronze.select(
        "id",
        "actor_id",
        "actor_login",
        "type",
        "ingest_hour",
        F.col("actor_login").endswith("[bot]").alias("rule_a_login_suffix"),
        rule_b_expr.alias("rule_b_github_app"),
    ).cache()

    total_events = flagged.count()
    total_actors = flagged.select("actor_id").distinct().count()

    rule_a_events = flagged.filter("rule_a_login_suffix").count()
    rule_b_events = flagged.filter("rule_b_github_app").count()
    either_events = flagged.filter("rule_a_login_suffix OR rule_b_github_app").count()
    both_events = flagged.filter("rule_a_login_suffix AND rule_b_github_app").count()

    rule_a_actors = (
        flagged.filter("rule_a_login_suffix").select("actor_id").distinct().count()
    )
    rule_b_actors = (
        flagged.filter("rule_b_github_app").select("actor_id").distinct().count()
    )
    either_actors = (
        flagged.filter("rule_a_login_suffix OR rule_b_github_app")
        .select("actor_id")
        .distinct()
        .count()
    )

    print("\n========== bot heuristic spike ==========")
    print(f"bronze path:        {args.bronze_path}")
    print(f"total events:       {total_events:,}")
    print(f"total actors:       {total_actors:,}")

    print("\n--- event counts ---")
    print(f"rule A only (login ~ [bot]):     {rule_a_events:,}")
    print(f"rule B only (performed_via_app): {rule_b_events:,}")
    print(f"either rule (A union B):         {either_events:,}")
    print(f"both rules (A intersect B):      {both_events:,}")
    print(f"either share of events:          {100 * either_events / total_events:.2f}%")

    print("\n--- distinct actor counts ---")
    print(f"rule A actors:  {rule_a_actors:,}")
    print(f"rule B actors:  {rule_b_actors:,}")
    print(f"either actors:  {either_actors:,}")
    print(f"either share of actors: {100 * either_actors / total_actors:.2f}%")

    print("\n--- top 20 actors by event count (manual label aid) ---")
    print("flag legend: A = login ~ [bot], B = performed_via_github_app non-null")
    top_actors = (
        flagged.groupBy("actor_login")
        .agg(
            F.count("*").alias("event_count"),
            F.max(F.col("rule_a_login_suffix").cast("int")).alias("rule_a"),
            F.max(F.col("rule_b_github_app").cast("int")).alias("rule_b"),
        )
        .orderBy(F.col("event_count").desc())
        .limit(20)
    )
    rows = top_actors.collect()
    print(f"  {'rank':>4}  {'login':<45} {'events':>8}  A  B")
    for rank, row in enumerate(rows, start=1):
        flag_a = "1" if row["rule_a"] else "."
        flag_b = "1" if row["rule_b"] else "."
        login = row["actor_login"] or "<null>"
        print(f"  {rank:>4}  {login:<45} {row['event_count']:>8,}  {flag_a}  {flag_b}")

    print("\n--- per-event-type breakdown of rule B (performed_via_github_app) ---")
    by_type = (
        flagged.filter("rule_b_github_app")
        .groupBy("type")
        .count()
        .orderBy(F.col("count").desc())
    )
    for row in by_type.collect():
        print(f"  {row['type']:<30}  {row['count']:>8,}")

    spark.stop()


if __name__ == "__main__":
    main()
