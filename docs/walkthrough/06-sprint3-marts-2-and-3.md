# Chapter 06 — Sprint 3:横向扩 Silver + Mart 2 和 3

## 这章你会学到什么

5 张新 Silver 表 + 2 张新 Gold mart + 一个**真实的 cross-mart bug 被 verifier 抓到**的故事。读完你能解释:**ADR-0005 的"需求驱动建 Silver"是什么意思,为什么用 `is_bot()` 宏比每个 mart 重写一遍 SQL 强,以及 cross-mart verifier 那次抓到的 LombiqBot 不一致是怎么回事——这是项目里最 senior 的瞬间**。

## 关联前后

- **上一章** ([Ch 05](05-sprint2.5-bot-spike.md)) 用 spike 决定了 bot 规则方向
- **下一章** ([Ch 07](07-sprint4-dq-airflow-runbooks.md)) 给整个项目加 DQ gate 和 Airflow

## 背景概念(30 秒补课)

- **Demand-driven build**:不一次性把所有可能用到的 Silver 表都建,只在某个 Gold mart 真需要时才建。"懒构建"。
- **first-response time**:某个 issue 被 open 后,作者以外的人第一次评论的时间间隔。OSS health 的关键指标之一。
- **window function**:SQL 的 `row_number() over (partition by ... order by ...)`、`min(x) over (...)` 这类按分组排序后做聚合的函数。
- **UNION ALL**:把多张表的行竖向拼起来,要求列名+类型一致。
- **cross-mart consistency**:两张 Gold mart 都有"bot push 数"概念,这两张表对同一个 (repo, day) 应该说同一个数字。

## 这一阶段的目标

Sprint 3 分两个子 Sprint:

**3a — OSS Health Mart**(PR / Issue 生命周期):
- 加 Silver: `events_pull_request`, `events_issues`, `events_issue_comment`
- 加 Gold: `oss_health_mart`(grain: `(repo_id, activity_date)`)

**3b — Bot Mart**(bot 占比):
- 加 Silver: `events_watch`, `events_fork`(让所有 event type 都能算 bot 占比)
- 加 dbt seed: `known_bots.csv`(Rule C 白名单)
- 加 dbt macro: `is_bot()`(集中 bot 规则)
- 加 Gold: `bot_vs_human_activity_mart`(用 6 张 Silver 表 UNION ALL)

## 设计决策怎么做的

### 决策 1:ADR-0005 demand-driven Silver

GH Archive 有 15 种 event type。一个"懒"的做法是一次性建 15 张 Silver。但每张 Silver 是一个**契约**——schema yml 要维护、`on_schema_change='fail'` 在 schema drift 时会触发、占 storage 和 build time。

**ADR-0005** 的决定:**只在某个 Gold mart 真要时建那张 Silver**。

| 用到的 event type | 谁要 |
|------------------|------|
| PushEvent | Sprint 2 Gold mart 1 |
| PullRequest, Issues, IssueComment | Sprint 3a Gold mart 2 |
| Watch, Fork | Sprint 3b Gold mart 3(cross-event 覆盖) |
| 剩下 9 种 | 没人要,不建 |

这个决定的硬度:如果某天有人 PR 想加 `events_member` 但说不出哪个 Gold mart 要它,**reviewer 应该拒**。

### 决策 2:`is_bot()` 用宏,不用 view 不用 inline

三个选项:

| 选项 | 优 | 劣 |
|------|----|----|
| inline 在每个 mart 的 `case when` 里 | 简单 | 两个 mart 重复代码,**容易漂移** |
| 建一个 `silver.actor_is_bot` view,join 进 mart | 漂移不再可能 | 多一次 join,影响性能 |
| **dbt macro `{{ is_bot('actor_login') }}`** | 单一来源 + 渲染成 inline SQL,无 join 开销 | 一段 Jinja,新人要懂 |

选 macro。**真正的 senior 论据**:Sprint 3b 的 cross-mart verifier 真的抓到了一次"两个 mart 用了不同 bot 定义"的 bug——下面会讲。如果用 inline,这个 bug 不会被 verifier 抓到,会一直存在。

### 决策 3:OSS Health Mart 的 first-response time 怎么算

Issue 被 open 后的"首响应"语义是:**第一条非 opener 评论的时间 - issue 创建时间**。

SQL 思路:

```sql
with issue_first_response as (
    select
        repo_id, issue_id, issue_opener_user_id, issue_created_at,
        min(case when comment_user_id != issue_opener_user_id
                 then comment_created_at end) as first_response_at
    from {{ ref('events_issue_comment') }}
    where action = 'created' and comment_created_at >= issue_created_at
    group by repo_id, issue_id, issue_opener_user_id, issue_created_at
)
```

要点:

- `min(case when ... then ... end)`:类似 conditional aggregation。只统计 "非 opener 的评论",取最早一条
- 按 `(repo_id, issue_id)` group,每个 issue 一行 first_response_at
- **已知 corner case**:如果 issue 真正的第一条非-opener 评论发生在 sample 窗口之外(比如 1 月发的 issue,我们 sample 是 2025-01-15 一天),我们只能看到"这天最早的"——会比真值偏大。我们在 design doc 明确写了这是 sample-window 局限

然后聚到 (repo, response_date) 算平均:

```sql
response_agg as (
    select
        repo_id,
        cast(first_response_at as date) as activity_date,
        avg((unix_timestamp(first_response_at) - unix_timestamp(issue_created_at)) / 3600.0)
            as issue_avg_first_response_hours
    from issue_first_response
    where first_response_at is not null
    group by repo_id, cast(first_response_at as date)
)
```

### 决策 4:Bot Mart 用 6-way UNION ALL

Bot 占比要看跨 event type——某 repo 推送很多但都是 bot,issue 很多但都是人,这种 mix 不在单一 Silver 里能算。所以 mart 的输入是"把 6 张 Silver 表拍扁成 (repo, date, actor, event_class) 长表":

```sql
unified as (
    select repo_id, actor_id, actor_login, created_at, 'push' as event_class
    from {{ ref('events_push') }}
    union all
    select repo_id, actor_id, actor_login, created_at, 'pr' as event_class
    from {{ ref('events_pull_request') }}
    union all
    -- 4 more event types...
)
```

然后在 `unified` 上 group by `(repo_id, date)` 算各种 metrics。

这 SQL 长但**很对称**——每个 event type 贡献的列是同样的 4 个。一个对 ADR-0005 的呼应:我们只 union 已经存在的 Silver,而不是从 Bronze 重新解析。

## 代码逐行讲

### `dbt/seeds/known_bots.csv`

```csv
login,note,added_date,added_by
LombiqBot,Custom-named bot from Lombiq; does not use [bot] suffix. Visible miss in Sprint 2.5 spike top-20.,2026-06-28,peter
```

一个 CSV。`dbt seed` 命令把它加载成一张表(`silver.known_bots`)。

**为什么 seed,不直接写宏里 hardcode 列表?** —— 因为 CSV 改动有 git diff,review 时清楚谁加了哪个 bot、为什么加。代码里 hardcode 一个 list 文件 history 也有,但 CSV 的"一行一条 +note 字段"格式天然提示要写 justification。

### `dbt/macros/is_bot.sql`

```jinja
{% macro is_bot(login_column) %}
    (
        {{ login_column }} like '%[bot]'
        or {{ login_column }} in (select login from {{ ref('known_bots') }})
    )
{% endmacro %}
```

用 Jinja 写,渲染成 inline SQL boolean expression。

- 第一行 `like '%[bot]'`:Rule A
- `in (select login from known_bots)`:Rule C
- **括号包起来**:重要——保证 macro 调用作为表达式不会被外层操作符抢优先级

用法在 mart SQL 里:

```sql
sum(case when {{ is_bot('actor_login') }} then 1 else 0 end) as bot_push_count
```

dbt 编译后变成:

```sql
sum(case when (actor_login like '%[bot]' or actor_login in (select login from silver.known_bots)) then 1 else 0 end) as bot_push_count
```

### `dbt/models/silver/events_pull_request.sql`(部分)

```sql
{{ config(materialized='incremental', unique_key='id',
          incremental_strategy='merge', file_format='delta',
          on_schema_change='fail') }}

with bronze_pr as (
    select id, actor_id, actor_login, repo_id, repo_name,
           org_id, org_login, is_public, created_at, ingest_hour, payload_raw
    from {{ delta_source('bronze', 'events') }}
    where type = 'PullRequestEvent'
    {% if is_incremental() %}
        and ingest_hour > (select coalesce(max(ingest_hour), '1970-01-01-00') from {{ this }})
    {% endif %}
),

parsed as (
    select
        id, actor_id, actor_login, repo_id, repo_name,
        org_id, org_login, is_public, created_at, ingest_hour,
        get_json_object(payload_raw, '$.action')                          as action,
        cast(get_json_object(payload_raw, '$.number')         as bigint)  as pr_number,
        cast(get_json_object(payload_raw, '$.pull_request.id') as bigint) as pr_id,
        get_json_object(payload_raw, '$.pull_request.state')              as pr_state,
        cast(get_json_object(payload_raw, '$.pull_request.merged') as boolean) as pr_merged,
        to_timestamp(get_json_object(payload_raw, '$.pull_request.created_at'),
                     "yyyy-MM-dd'T'HH:mm:ss'Z'")                          as pr_created_at,
        to_timestamp(get_json_object(payload_raw, '$.pull_request.closed_at'),
                     "yyyy-MM-dd'T'HH:mm:ss'Z'")                          as pr_closed_at,
        to_timestamp(get_json_object(payload_raw, '$.pull_request.merged_at'),
                     "yyyy-MM-dd'T'HH:mm:ss'Z'")                          as pr_merged_at,
        cast(get_json_object(payload_raw, '$.pull_request.user.id') as bigint) as pr_user_id,
        -- ...
    from bronze_pr
)

select * from parsed
```

模板跟 `events_push.sql` 完全相同的形状(ADR-0005 的"统一模板"原则)。只是字段不同:

- `action`:`opened` / `closed` / `merged` / `reopened`
- `pr_id`:**注意!这跟 envelope `id` 不一样**。`id` 是事件 ID(每个 action 一个),`pr_id` 是 PR 实体 ID(整个 PR 生命周期一个)
- 3 个时间戳:created_at / closed_at / merged_at
- `pr_merged`:boolean,这条 event 是否是 "closed-as-merged"

### `dbt/models/gold/oss_health_mart.sql`(关键聚合段)

```sql
pr_agg as (
    select
        repo_id, activity_date,
        sum(case when action = 'opened' then 1 else 0 end) as pr_opened_count,
        sum(case when action = 'closed' then 1 else 0 end) as pr_closed_count,
        sum(case when action = 'closed' and pr_merged then 1 else 0 end) as pr_merged_count,
        avg(case when pr_merged and pr_merged_at is not null and pr_created_at is not null
                 then (unix_timestamp(pr_merged_at) - unix_timestamp(pr_created_at)) / 3600.0
            end) as pr_avg_merge_latency_hours
    from pr_events
    group by repo_id, activity_date
),
```

读这段:

- 三个 count 用 `case when ... then 1 else 0 end + sum`,这是 SQL 算条件计数的标准写法
- `pr_avg_merge_latency_hours`:只对"已 merge 的 PR"算时差,**`avg(case when ... then x end)`** 会自动忽略 null。如果当天没 merge 任何 PR,结果是 null(不是 0,有语义区别)
- `/ 3600.0`:从秒转小时。`3600.0` 不是 `3600`——确保整数除法不发生

### `dbt/models/gold/bot_vs_human_activity_mart.sql`(主聚合)

```sql
classified as (
    select
        repo_id,
        cast(created_at as date) as activity_date,
        actor_id, actor_login, event_class,
        case when {{ is_bot('actor_login') }} then 1 else 0 end as is_bot_flag
    from unified
),

aggregated as (
    select
        repo_id, activity_date,
        count(*)                                                              as event_count,
        sum(is_bot_flag)                                                      as bot_event_count,
        sum(case when is_bot_flag = 0 and actor_login is not null then 1 else 0 end) as human_event_count,

        sum(case when is_bot_flag = 1 and event_class = 'push'    then 1 else 0 end) as push_bot_count,
        sum(case when is_bot_flag = 1 and event_class = 'pr'      then 1 else 0 end) as pr_bot_count,
        -- ... 4 more event_class buckets ...

        count(distinct case when is_bot_flag = 1 then actor_id end) as distinct_bot_actors,
        count(distinct case when is_bot_flag = 0 then actor_id end) as distinct_human_actors
    from classified
    group by repo_id, activity_date
)
```

要点:

- `classified` 一次性给每行打 `is_bot_flag`,**只 invoke is_bot macro 一次**(不是后面每个聚合都 invoke)
- `count(distinct case when ... then actor_id end)`:**count distinct 配 case-when 的非常 senior 的写法**——在同一个 aggregate 里同时算 bot 和 human 的 distinct,只过一遍数据

### `spark/jobs/gold_bot_verify.py`(关键剧本)

```python
def main():
    spark = build_spark()
    bot = spark.read.format("delta").load(BOT_PATH)
    act = spark.read.format("delta").load(ACT_PATH)

    # Check 1: grain invariant
    total = bot.count()
    unique = bot.select("repo_id", "activity_date").distinct().count()
    if total != unique:
        raise SystemExit(1)

    # Check 2: bot+human <= total
    bad = bot.filter(F.col("bot_event_count") + F.col("human_event_count") > F.col("event_count")).count()
    if bad > 0:
        raise SystemExit(2)

    # Check 3: cross-mart — push_bot_count vs bot_push_count
    joined = bot.select("repo_id", "activity_date", "push_bot_count").join(
        act.select("repo_id", "activity_date", "bot_push_count"),
        on=["repo_id", "activity_date"], how="inner",
    )
    mismatches = joined.filter(F.col("push_bot_count") != F.col("bot_push_count")).count()
    if mismatches > 0:
        print("[cross-mart] MISMATCH detected")
        mismatches.show(5)
        raise SystemExit(3)
```

**这就是项目里 senior signal 最强的一段。**

第一次跑 Sprint 3b 完之后,这个 Check 3 输出了:

```
[cross-mart] push_bot_count != bot_push_count rows: 108
+--------+-------------+--------------+--------------+
|repo_id |activity_date|push_bot_count|bot_push_count|
+--------+-------------+--------------+--------------+
|46566064|2018-01-15   |43            |0             |
|46588421|2018-01-15   |16            |0             |
|46592776|2018-01-15   |26            |0             |
...
```

**108 行不一致**。原因:

- Sprint 2 的 `repo_daily_activity.bot_push_count` 是用 inline `actor_login like '%[bot]'`(只 Rule A)
- Sprint 3b 的 `bot_vs_human_activity_mart.push_bot_count` 是用 `is_bot()`(Rule A + Rule C 含 LombiqBot)
- 2018-01-15 那天 LombiqBot 推了几十次 → 一个 mart 算 0,另一个算 43

**这就是为什么要用宏统一规则**。修复:把 `repo_daily_activity.sql` 里的 inline 换成 `{{ is_bot('actor_login') }}`,full-refresh。再跑 verifier:

```
[cross-mart] push_bot_count != bot_push_count rows: 0
[cross-mart] push bot counts match repo_daily_activity exactly
```

**这一刻是 Sprint 3b 的真实价值。** 不是"bot mart 上线",而是"我自己写的 verifier 抓住我自己的 bug,逼我把规则收敛到一个宏"。

## 验证 — 这阶段怎么知道做对了

所有 mart build + 全部 dbt test pass(58 个 test):

```bash
JAVA_HOME=... ../.venv/bin/dbt build --select gold
# Done. PASS=18 (oss_health), PASS=27 (bot mart), PASS=13 (repo_daily_activity refreshed)

JAVA_HOME=... uv run python -m spark.jobs.gold_health_verify
# [ground truth] all merged_count match: True; latency match (1ms): True

JAVA_HOME=... uv run python -m spark.jobs.gold_bot_verify
# [cross-mart] joined=162,719  mismatches=0
```

最后一行 `mismatches=0` 是 senior 必看的数字。

## 代码 review 笔记

复看 `bot_vs_human_activity_mart.sql`,有一处可以更清晰:6 个 UNION ALL block 几乎重复(只有 `event_class` 字符串和表名变)。可以用 dbt macro 一段 for-loop 生成:

```jinja
{% set sources = [
    ('events_push', 'push'), ('events_pull_request', 'pr'),
    ('events_issues', 'issue'), ('events_issue_comment', 'comment'),
    ('events_watch', 'watch'), ('events_fork', 'fork')
] %}
{% for table, cls in sources %}
    select repo_id, actor_id, actor_login, created_at, '{{ cls }}' as event_class
    from {{ ref(table) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
```

更 DRY。**但我们保留 6 个显式 block**,理由:

1. 显式比 Jinja 循环易读 (junior 团队成员)
2. 每个 event type 加 `events_fork` 时 source_repo_id 是不同的字段名(我们要 alias 成 `repo_id`)——for-loop 处理不了这种 special case 需要再加 if 分支,变得更绕
3. 6 行 boilerplate 不值得 DRY

我面试时这么讲:**"I considered a Jinja loop. Rejected because event_fork's source_repo_id needed an alias the loop couldn't express cleanly. Six explicit blocks beat one clever macro."**

## You will be able to say

### 3-minute version (English)

> "Sprint 3 has two sub-sprints. 3a builds the OSS Health mart from
> three new Silver tables — `events_pull_request`, `events_issues`,
> `events_issue_comment`. 3b builds the Bot vs Human mart from those
> three plus `events_watch` and `events_fork`, with a curated bot
> allowlist seed and an `is_bot()` macro that centralizes the rule.
>
> Two design ideas worth talking about. First, ADR-0005 says Silver
> tables get built only when a Gold mart needs them. I rejected
> building all 15 event types up front — each table is a contract,
> and unused tables are pure cost. Second, the `is_bot()` dbt macro
> renders inline SQL but lives in one file, so two consuming marts
> can't disagree on the rule.
>
> The strongest senior signal in the project is what happened next.
> The cross-mart verifier `gold_bot_verify.py` checks that
> `repo_daily_activity.bot_push_count` equals
> `bot_vs_human_activity_mart.push_bot_count` for every joined
> repo-day. First run after Sprint 3b: 108 mismatches. Why? Because
> Sprint 2 had written the bot rule inline as a `like '%[bot]'`
> predicate. Sprint 3b's macro added Rule C — the allowlist —
> which catches `LombiqBot`. The two marts disagreed for 108
> repo-days where LombiqBot was active.
>
> Fix: change Sprint 2's mart to use the same `is_bot()` macro,
> full-refresh, re-run verifier — zero mismatches. The lesson is
> not 'bot detection got better'. It's that the verifier I wrote
> in Sprint 3b caught my own inconsistency from Sprint 2, and forced
> me to converge both marts on the macro. That kind of self-policing
> tool is the difference between 'I shipped two marts' and 'I shipped
> two marts that I can prove agree'."

## 常见尖刻问题 + 准备好的答案

**Q: "你怎么知道你不会再次漂移?"**

> "I can't prevent future drift in code, but I can detect it on
> every run. The cross-mart check is wired into `quality.runner
> --layer cross_mart`, which the Airflow DAG runs after Gold build.
> Any future PR that diverges the bot rule between marts gets
> caught at gate time, before the run finishes."

**Q: "Rule C 白名单有几条?能扩到多大?"**

> "One entry today, LombiqBot. The expected long-term size is dozens,
> not thousands — most bots follow GitHub's `[bot]` convention.
> Each addition goes through PR review with a justification note.
> If it grows past say 100, I'd reconsider whether a different
> detection signal is warranted."

**Q: "OSS Health mart 的 first_response_time 在 sample 之外的数据怎么办?"**

> "Already documented as a sample-window limitation in the mart's
> design doc. The metric reports 'first response we observed', not
> 'first response in absolute time'. Full backfill of multi-day
> data would resolve it, and Sprint 4's Airflow DAG is the
> infrastructure for that."

---

下一章 →  [07-sprint4-dq-airflow-runbooks.md](07-sprint4-dq-airflow-runbooks.md)
