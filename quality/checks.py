"""Data-quality gates for OSS Pulse Bronze / Silver / Gold layers.

Each check is a function returning a CheckResult. Checks are grouped
into "suites" (per layer) executed by `quality.runner.run_suite`.

Design notes
------------
We deliberately do NOT use the full Great Expectations framework here.
The substance we need from GE — a defined set of expectations,
pass/fail with detail, and a CLI that returns non-zero on failure so
Airflow can gate downstream — is ~150 lines of plain Python. The GE
overhead (YAML config, DataContext setup, HTML data-docs) would
exceed the value at this project scale and lock the project into a
heavy dep tree.

The structure here mirrors a GE checkpoint:
    - `suite_<layer>`  ~ ExpectationSuite
    - each function    ~ Expectation
    - `CheckResult`    ~ ExpectationValidationResult

If at Sprint 5+ we need profiling / HTML docs / out-of-the-box expectations
that aren't worth re-implementing, swapping these check functions into
GE expectations is a one-day port.

Convention: every check returns (name, passed, details). They never
raise — `runner.run_suite` is the only place that decides what to do
with a failure. This makes unit testing checks trivial.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


KNOWN_EVENT_TYPES: set[str] = {
    "PushEvent",
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "IssuesEvent",
    "IssueCommentEvent",
    "WatchEvent",
    "ForkEvent",
    "CreateEvent",
    "DeleteEvent",
    "ReleaseEvent",
    "MemberEvent",
    "PublicEvent",
    "GollumEvent",
    "CommitCommentEvent",
}


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str

    def __str__(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        return f"[{mark}] {self.name} — {self.details}"


# ------------------------------------------------------------------ Bronze


def bronze_id_unique_and_not_null(bronze: DataFrame) -> CheckResult:
    total = bronze.count()
    distinct = bronze.select("id").distinct().count()
    nulls = bronze.filter(F.col("id").isNull()).count()
    passed = (total == distinct) and (nulls == 0)
    return CheckResult(
        name="bronze.events.id is unique and not_null",
        passed=passed,
        details=f"total={total:,}, distinct={distinct:,}, nulls={nulls}",
    )


def bronze_type_in_known_set(bronze: DataFrame) -> CheckResult:
    observed = {r["type"] for r in bronze.select("type").distinct().collect()}
    unexpected = observed - KNOWN_EVENT_TYPES
    return CheckResult(
        name="bronze.events.type only in known set",
        passed=not unexpected,
        details=(
            f"observed={len(observed)} types"
            + (f", unexpected={sorted(unexpected)}" if unexpected else "")
        ),
    )


def bronze_is_public_always_true(bronze: DataFrame) -> CheckResult:
    bad = bronze.filter((F.col("is_public").isNull()) | (F.col("is_public") == False)).count()  # noqa: E712
    return CheckResult(
        name="bronze.events.is_public == true for every row (ADR-0001 contract)",
        passed=bad == 0,
        details=f"non-true rows={bad}",
    )


def bronze_created_at_not_null(bronze: DataFrame) -> CheckResult:
    bad = bronze.filter(F.col("created_at").isNull()).count()
    return CheckResult(
        name="bronze.events.created_at not_null",
        passed=bad == 0,
        details=f"null rows={bad}",
    )


# ------------------------------------------------------------------ Silver


def silver_row_count_matches_bronze(
    silver: DataFrame, bronze: DataFrame, event_type: str, silver_name: str
) -> CheckResult:
    silver_count = silver.count()
    bronze_count = bronze.filter(F.col("type") == event_type).count()
    return CheckResult(
        name=f"silver.{silver_name} row count == bronze.events where type='{event_type}'",
        passed=silver_count == bronze_count,
        details=f"silver={silver_count:,}, bronze_filtered={bronze_count:,}",
    )


def silver_pr_state_in_known(silver_pr: DataFrame) -> CheckResult:
    observed = {
        r["pr_state"]
        for r in silver_pr.select("pr_state").distinct().collect()
        if r["pr_state"] is not None
    }
    unexpected = observed - {"open", "closed"}
    return CheckResult(
        name="silver.events_pull_request.pr_state in {open, closed}",
        passed=not unexpected,
        details=f"observed={sorted(observed)}"
        + (f", unexpected={sorted(unexpected)}" if unexpected else ""),
    )


# ------------------------------------------------------------------ Gold


def gold_grain_unique(
    gold: DataFrame, grain: tuple[str, ...], mart_name: str
) -> CheckResult:
    total = gold.count()
    unique = gold.select(*grain).distinct().count()
    return CheckResult(
        name=f"gold.{mart_name} grain {grain} unique",
        passed=total == unique,
        details=f"total={total:,}, unique={unique:,}",
    )


def gold_health_pr_merged_le_closed(health: DataFrame) -> CheckResult:
    bad = health.filter(F.col("pr_merged_count") > F.col("pr_closed_count")).count()
    return CheckResult(
        name="oss_health_mart.pr_merged_count <= pr_closed_count",
        passed=bad == 0,
        details=f"violating rows={bad}",
    )


def gold_bot_share_in_range(bot: DataFrame) -> CheckResult:
    bad = bot.filter(
        (F.col("bot_event_share") < 0) | (F.col("bot_event_share") > 1)
    ).count()
    return CheckResult(
        name="bot_vs_human_activity_mart.bot_event_share in [0, 1]",
        passed=bad == 0,
        details=f"out-of-range rows={bad}",
    )


def cross_mart_bot_push_count_agrees(
    activity: DataFrame, bot: DataFrame
) -> CheckResult:
    """Cross-mart gate caught by Sprint 3b — bot rule must be
    identical in repo_daily_activity and bot_vs_human_activity_mart."""
    joined = activity.select("repo_id", "activity_date", "bot_push_count").join(
        bot.select("repo_id", "activity_date", "push_bot_count"),
        on=["repo_id", "activity_date"],
        how="inner",
    )
    mismatches = joined.filter(
        F.col("bot_push_count") != F.col("push_bot_count")
    ).count()
    joined_count = joined.count()
    return CheckResult(
        name="cross-mart: repo_daily_activity.bot_push_count == bot_mart.push_bot_count",
        passed=mismatches == 0,
        details=f"joined={joined_count:,}, mismatches={mismatches}",
    )
