# Chapter 04 — Sprint 2:第一个 Gold mart

## 这章你会学到什么

从 Silver 一张表造出第一张面向业务的 Gold mart。读完你能解释:**什么是 composite key、为什么这个项目敢不引入 surrogate key、为什么"用宏 override schema 名"是个看似小事但实际关键的设计、cross-layer 验证脚本(`gold_verify.py`)为什么不能用 dbt schema test 替代**。

## 关联前后

- **上一章** ([Ch 03](03-sprint1-first-silver.md)) 完成了 Silver 第一张表 `events_push`
- **下一章** ([Ch 05](05-sprint2.5-bot-spike.md)) 在动 Gold mart #2 之前做了一个关键 spike

## 背景概念(30 秒补课)

- **Gold mart**:面向业务用例的最终聚合表。Dashboard、API、报表直接读。一张 Gold 通常解决一个业务问题。
- **Composite key**:用多个字段一起做主键。比如 `(repo_id, activity_date)` 唯一确定一行——同一个 repo 在同一天只有一行。
- **Surrogate key**:不用业务字段做主键,生成一个无业务含义的 ID(自增 int、UUID、hash)。Kimball 经典数据仓库做法。
- **Composite key vs surrogate**:都能保证主键唯一。区别:surrogate 是额外列、跟业务无关、当业务键不稳定时有价值;composite 是业务键本身、读起来直观。我们的项目敢用 composite,是因为 `repo_id` 是 GitHub 永不变的 ID。
- **`dbt_utils`**:dbt 的第三方 macro 包,提供 `unique_combination_of_columns`(组合键唯一性测试)、`expression_is_true`(自定义布尔测试)等常用 test。

## 这一阶段的目标

造出 `gold.repo_daily_activity`,一张 mart,grain 是 `(repo_id, activity_date)`,每行表示某个 repo 在某天的活动总览:

| Column | 类型 | 含义 |
|--------|------|------|
| `repo_id` | bigint | GitHub 仓库 ID(grain) |
| `activity_date` | date | UTC 日期(grain) |
| `repo_name` | string | 当天最后看到的 repo 名(rename 用最新名字) |
| `org_id` / `org_login` | bigint / string | 所属 org |
| `push_count` | bigint | 当天推送事件数 |
| `total_commits` | bigint | sum(payload.size) |
| `distinct_commits` | bigint | sum(payload.distinct_size)(GitHub 自己 dedup 过的) |
| `unique_pushers` | bigint | count(distinct actor_id) |
| `bot_push_count` | bigint | actor_login 匹配 bot 规则的推送数 |
| `non_bot_push_count` | bigint | `push_count - bot_push_count` |

业务问题:**"昨天哪些 repo 最活跃,bot 占比多少?"**

## 设计决策怎么做的

### 决策 1:用 composite key 还是 surrogate key

经典 Kimball 教材会推 surrogate key。我们不引入,理由:

| Kimball 引入 surrogate 的常见动机 | 在 OSS Pulse 里成立吗 |
|----------------------------------|----------------------|
| 自然键不稳定(可能变) | GitHub `repo_id` 一旦创建永不变。`repo_name` 会变但我们不用 name 做 key |
| 跨多个数据源,自然键冲突 | 单数据源(GH Archive) |
| 自然键是 STRING,join 慢 | `repo_id` 是 BIGINT,join 快 |
| SCD-2(history tracking) | 这个 mart 不需要 history;只需要 latest |

四个动机一个都不成立。强行加 surrogate 反而:

- 多一列空间
- join 时多一跳(先查 dim_repo 的 surrogate,再 join fact)
- 增加"生成 surrogate 失败"的故障模式
- 反而把"我用了 GitHub 的 repo_id"这个事实藏起来,审计变难

**ADR-0004** 正式记录了这个决定。这是项目里我**主动拒绝**一个 textbook recommendation 的地方,senior signal 满分。

### 决策 2:`activity_date` 怎么算

候选:

- (a) `DATE(created_at)`:事件发生那一天
- (b) `DATE(ingest_hour)`:数据落地那一天

选 (a)。理由:**业务上 "1月15日的 push 次数" 是问"1月15日发生的事",不是问"1月15日我们写进数据库的事"**。

但这带来一个 corner case:23:59 UTC 发生的 push 可能在 00:30 UTC 才被 GH Archive 收录(下一天的 ingest_hour 处理它)。我们的 incremental cutoff 用 `activity_date > max(activity_date)`,这种 late-arriving 行会被静默丢弃。

**这是我们当下没解决,但写进 design doc 的已知缺陷**(`docs/marts/gold_repo_daily_activity.md`)。Sprint 4 的 Airflow DAG 设计会处理(look-back window)。**承认局限,不假装完美**——senior signal。

### 决策 3:`bot_push_count` 的 bot 规则放哪

Sprint 2 阶段我们只有 Rule A(`actor_login like '%[bot]'`)。最早的写法是直接 inline 在 model 里:

```sql
sum(case when actor_login like '%[bot]' then 1 else 0 end) as bot_push_count
```

Sprint 3b 引入 `is_bot()` macro 后,改成:

```sql
sum(case when {{ is_bot('actor_login') }} then 1 else 0 end) as bot_push_count
```

为什么改?**因为 Sprint 3b 的 `bot_vs_human_activity_mart` 用 Rule A + Rule C,如果两个 mart 用不同规则,数据会不一致——而 cross-mart verifier 真的抓到了这个不一致**(详见 [Ch 06](06-sprint3-marts-2-and-3.md))。

这就是宏的真正价值:**单一来源(single source of truth),两个消费者不可能漂移**。

### 决策 4:dbt schema 名怎么管理

dbt 默认行为:你写 `+schema: gold`,实际生成的 schema 名是 `{target.schema}_gold`,比如 `silver_gold`。这是为了同一个 warehouse 上多个 dev 不冲突。

但我们要的就是 schema 名叫 `gold`,不是 `silver_gold`。解决方案:重写 `generate_schema_name` 宏。

```jinja
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

逻辑:**自定义 schema 名时直接用 verbatim,不加前缀**。

这是 dbt 文档明确支持的覆盖,一段 5 行 Jinja。**重要的不是这段代码,而是"我看到默认行为不符合需求,然后用 framework 提供的扩展点去改它"**。这是 senior 在 framework 里工作的基本动作。

## 代码逐行讲

### `dbt/packages.yml` + `dbt deps`

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

跑 `dbt deps` 把 dbt-utils 装进 `dbt_packages/`(gitignored)。装这个包之后才能用 `dbt_utils.unique_combination_of_columns` 这种 macro。

### `dbt/seeds/known_bots.csv`(Sprint 3b 才加的,提前预告)

```csv
login,note,added_date,added_by
LombiqBot,Custom-named bot from Lombiq; does not use [bot] suffix. Visible miss in Sprint 2.5 spike top-20.,2026-06-28,peter
```

这是 dbt seed —— 一个 CSV 文件,跑 `dbt seed` 会把它当成一张表加载到 warehouse。允许 PR 流程下增减(每条改动都有 commit 历史 + reviewer)。

### `dbt/models/gold/repo_daily_activity.sql`(主角)

```sql
{{
    config(
        materialized='incremental',
        unique_key=['repo_id', 'activity_date'],
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

with push as (
    select
        repo_id,
        repo_name,
        org_id,
        org_login,
        actor_id,
        actor_login,
        id           as event_id,
        commit_size,
        distinct_commit_size,
        cast(created_at as date) as activity_date
    from {{ ref('events_push') }}

    {% if is_incremental() %}
        where cast(created_at as date) > (
            select coalesce(max(activity_date), date('1970-01-01'))
            from {{ this }}
        )
    {% endif %}
),

aggregated as (
    select
        repo_id,
        activity_date,
        max(repo_name)            as repo_name,
        max(org_id)               as org_id,
        max(org_login)            as org_login,
        count(event_id)           as push_count,
        sum(commit_size)          as total_commits,
        sum(distinct_commit_size) as distinct_commits,
        count(distinct actor_id)  as unique_pushers,
        sum(case when {{ is_bot('actor_login') }} then 1 else 0 end) as bot_push_count
    from push
    group by repo_id, activity_date
)

select
    repo_id,
    activity_date,
    repo_name,
    org_id,
    org_login,
    push_count,
    total_commits,
    distinct_commits,
    unique_pushers,
    bot_push_count,
    push_count - bot_push_count as non_bot_push_count
from aggregated
```

读这段:

- `unique_key=['repo_id', 'activity_date']`:**composite key 用 list 表达**。这是 dbt MERGE 拼 ON 条件的关键
- 顶部 CTE `push`:从 Silver `events_push` 选需要的列。`{{ ref('events_push') }}` 是 dbt 解析的——它会自动建立 model 依赖图,知道这个 Gold 依赖那个 Silver,先 build Silver 再 build Gold
- 增量 cutoff 用 `activity_date > max(activity_date)`,跟前文说的 late-arriving 局限呼应
- `aggregated` CTE:`group by repo_id, activity_date` 是 grain 的物理表达
- `max(repo_name)`:repo 重命名场景下取"按字典序最大的名字"(不严格是 latest,但实践上 OK——重命名极少发生在同一天)
- `max(org_id)` 同理
- `sum(case when {{ is_bot(...) }} then 1 else 0 end)`:用宏调用,**保证两个 mart 永不漂移**
- 最后 `non_bot_push_count = push_count - bot_push_count`:派生字段,**让 schema test 能直接验证这个等式**

### `dbt/models/gold/_gold_schema.yml`

```yaml
version: 2

models:
  - name: repo_daily_activity
    description: |
      Gold: one row per (repo_id, activity_date). Daily PushEvent rollup
      with bot/non-bot split. ...
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - repo_id
            - activity_date
    columns:
      - name: repo_id
        description: GitHub repository id (stable across renames, ADR-0004)
        tests:
          - not_null
      - name: activity_date
        tests:
          - not_null
      - name: push_count
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      # ...
      - name: non_bot_push_count
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "= push_count - bot_push_count"
```

逐项:

- **model 层** 的 `unique_combination_of_columns` 是 composite grain 的 test。如果 `(repo_id, activity_date)` 重复,这个 test fail
- 每个 metric 加 `expression_is_true(">= 0")`——这种"counts 不该为负"的 sanity check 很便宜
- `non_bot_push_count = push_count - bot_push_count` 用 expression test 直接验证派生字段的逻辑

### `spark/jobs/gold_verify.py`(cross-layer 验证)

这个不是 dbt 的 schema test 能做的事。它做两件 dbt test 做不到的:

```python
def main():
    spark = build_spark()
    gold = spark.read.format("delta").load(GOLD_PATH)
    silver = spark.read.format("delta").load(SILVER_PATH)

    # Check 1: grain invariant
    total = gold.count()
    unique_grain = gold.select("repo_id", "activity_date").distinct().count()
    invariant_ok = total == unique_grain
    if not invariant_ok:
        raise SystemExit(1)

    # Check 2: cross-layer ground truth
    busiest = gold.orderBy(F.col("push_count").desc()).limit(1).collect()[0]
    repo_id = busiest["repo_id"]
    activity_date = busiest["activity_date"]

    recomputed = silver.filter(
        (F.col("repo_id") == repo_id)
        & (F.to_date("created_at") == F.lit(activity_date))
    ).agg(
        F.count("*").alias("push_count"),
        F.sum("commit_size").alias("total_commits"),
        F.countDistinct("actor_id").alias("unique_pushers"),
        F.sum(F.when(F.col("actor_login").endswith("[bot]"), 1).otherwise(0)).alias("bot_push_count"),
    ).collect()[0]

    fields = ["push_count", "total_commits", "unique_pushers", "bot_push_count"]
    mismatches = [f for f in fields if busiest[f] != recomputed[f]]
    if mismatches:
        raise SystemExit(2)
```

要点:

1. **Grain invariant 双重保险**。dbt 的 `unique_combination_of_columns` test 已经查过一次,这里我用纯 Spark 又查一次。两套独立的 code path 验证同一个 invariant——senior signal
2. **跨层 ground truth**:从 Gold 拿"最忙的 row",回到 Silver 重新算这一行的 metrics,要求完全一致。这是 dbt schema test 永远做不到的事——因为 dbt test 只 query 一张表,这里要 cross-table 重算
3. **真实跑的 output**:`frdpzk2/ppub` 在 2025-01-15 push 2,672 次。Silver 重算也是 2,672。**全对**。这个 print 一次就是项目的活 log

## 验证 — 这阶段怎么知道做对了

```bash
cd dbt
JAVA_HOME=... PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell" \
  ../.venv/bin/dbt build --select gold.repo_daily_activity
# 13/13 tests pass

cd ..
JAVA_HOME=... uv run python -m spark.jobs.gold_verify
# [invariant] grain holds (total == unique): True
# [ground truth] all four metrics match silver: True
# [summary] gold.repo_daily_activity passes Sprint 2 step 4.
```

162,719 行,grain 唯一,跨层验证通过。

## 代码 review 笔记

复看 model 时发现一处可优化:`aggregated` 这个 CTE 没必要拆。整个 SQL 可以压平成一个 SELECT。但**保留 CTE 的理由是可读性**——"先 push,再 aggregated,再选输出列"的三步节奏对读 SQL 的人友好。这跟 Ch 03 events_push.sql 是同样的设计哲学。

更值得提的是 `max(repo_name) as repo_name` ——**严格说是 wrong**。如果 repo 在 2025-01-15 这一天被重命名(从 `oldname` 到 `newname`),`max()` 会取字典序大的那个,不是真"最后一次"。要严格"最新的 repo_name",得用 `last_value` 窗口函数或者 `argmax(repo_name, created_at)`。

但我们**保留 `max`**,因为:

1. 同一天 rename 极少
2. 即使发生,后续天就用新名字了——chronicled 错就一天
3. 严格的 `last_value` 写法多 5 行 SQL

这又是 trade-off 取舍。面试官问到 `max(repo_name)` 是不是 wrong,你的答案:**"是,严格不对。我考虑过 last_value 但选择 simplicity over precision,因为代价是单天的轻微 imprecision,业务影响近零。如果业务报错,我会换成 last_value——design doc 里这条已经记录为 known limitation。"**

## You will be able to say

### 2-minute version (English)

> "Sprint 2 ships the first Gold mart, `repo_daily_activity`. Grain
> is `(repo_id, activity_date)`. The decision worth talking about
> is ADR-0004: no surrogate keys. I evaluated the four classic
> Kimball motivations for introducing surrogates — unstable natural
> key, cross-source conflicts, slow STRING joins, SCD-2 history —
> and none apply here. GitHub `repo_id` is permanent and BIGINT,
> single-source data, no history tracking needed. So I use the
> natural key. The ADR has a revisit clause for when any of those
> conditions changes.
>
> The model is incremental with `unique_key=['repo_id',
> 'activity_date']` — composite key in dbt's incremental merge.
> Tests include dbt_utils.unique_combination_of_columns for the
> composite grain, plus runtime invariants in `gold_verify.py`
> that go further than dbt tests — it picks the busiest row in
> the Gold mart and recomputes its metrics straight from Silver.
> Frdpzk2/ppub on 2025-01-15: 2,672 pushes, matches Silver
> exactly. That's the cross-layer evidence that the aggregation
> is correct."

## 常见尖刻问题 + 准备好的答案

**Q: "为什么不引入 dim_repo 维度表?"**

> "Because no metric requires repo *history* yet. `repo_daily_activity`
> just needs the current repo name, which I denormalize as
> `max(repo_name)`. The day a Gold mart needs repo's creation date,
> primary language, star count snapshots — at that point a
> dim_repo with SCD-2 is the right structure, and ADR-0004 has a
> revisit clause for exactly this case. Until then, denormalized
> is simpler."

**Q: "`max(repo_name)` 不严格对吧?"**

> "Correct, it's not strictly the latest. I considered `last_value`
> over created_at ordering but chose simplicity. The cost is single-day
> imprecision when a repo is renamed mid-day — extremely rare in
> our sample. If business reports a real issue, I'd switch. The
> known-limitation is documented in the mart design doc."

**Q: "你用 dbt-utils 不算太重?"**

> "dbt-utils is the canonical extension for dbt — community-vetted,
> stable API. The two macros I use, `unique_combination_of_columns`
> and `expression_is_true`, would each take ~30 lines to reimplement.
> Pulling a small dep beats reimplementing twice."

**Q: "Gold 增量 cutoff 用 `activity_date > max(...)`,有 late-arriving 漏判?"**

> "Yes — it's documented. A PushEvent that happens at 23:59 UTC of
> day D and lands in the ingest_hour of day D+1 would have
> activity_date=D, but the cutoff would have moved past D once
> D+1's data came in. Sprint 4's Airflow DAG design will either
> widen the cutoff to last 2 days or re-process yesterday's
> partition. Currently it's a known limitation; the sample data
> doesn't trigger it because we always ingest full hours."

---

下一章 →  [05-sprint2.5-bot-spike.md](05-sprint2.5-bot-spike.md)
