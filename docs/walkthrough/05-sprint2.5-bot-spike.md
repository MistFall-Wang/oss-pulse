# Chapter 05 — Sprint 2.5:Bot 启发式 spike

## 这章你会学到什么

为什么 senior 在动 mart 之前要先做一个 spike,什么是 spike,以及一个"实测后假设被证伪"的真实案例。读完你能解释:**为什么 Rule B(`payload.performed_via_github_app`)看起来对、实测后我们却放弃了它,以及这个发现怎么改写了 ADR-0006**。

## 关联前后

- **上一章** ([Ch 04](04-sprint2-first-gold-mart.md)) 完成了 Gold mart #1,Sprint 2 闭环
- **下一章** ([Ch 06](06-sprint3-marts-2-and-3.md)) 用这章的 finding 真正落 Sprint 3 的 bot mart 和 ADR-0006

## 背景概念(30 秒补课)

- **Spike**:敏捷开发术语。time-boxed(通常 1-3 天)的探索性工作,目的是回答一个具体技术问题,产出**不是产品代码**,是文档或决定。Senior 用 spike 验证假设,junior 直接动手写代码然后返工。
- **Rule A**:`actor.login` 以 `[bot]` 结尾 → bot
- **Rule B(我们当时计划的)**:`payload.performed_via_github_app` 非空 → bot
- **Rule C(spike 后引入的)**:在白名单 CSV 里 → bot

## 这一阶段的目标

Sprint 3 的 `bot_vs_human_activity_mart` 需要"如何判断一条 event 是不是 bot 发的"。原 plan 写的是 Rule A + Rule B。但**先不要动 mart**,先用一天验证:

1. Rule A 在真实数据里 hit 多少?
2. Rule B 在真实数据里 hit 多少?
3. 两个规则的 overlap?
4. **如果只用 Rule A,会漏掉哪些 obvious bot?**

如果两个规则合起来覆盖率 < 90%,我们就知道 ADR-0006 得重写。

**这个 spike 不会 1 天后产生新代码进 production——它产生一份 finding doc 和一个升级版的 ADR。**

## 设计决策怎么做的(spike 前的纸面假设)

原 plan 假设 Rule A ∪ Rule B 覆盖大部分 bot。理由都是常识:

- `[bot]` 后缀是 GitHub 给 App-installed 账号的强制约定
- `payload.performed_via_github_app` 字段在 GH Archive 文档里说是 "this event was performed via a GitHub App"

两个规则听起来正交、互补。**但我们没在真实数据里验证过**。

## 代码逐行讲 — `spark/jobs/bot_heuristic_spike.py`

```python
"""Sprint 2.5 spike: validate proposed bot-identification heuristics."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = build_spark()
    bronze = spark.read.format("delta").load("data/bronze/events")

    # 两个规则的初版
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
        "id", "actor_id", "actor_login", "type", "ingest_hour",
        F.col("actor_login").endswith("[bot]").alias("rule_a_login_suffix"),
        rule_b_expr.alias("rule_b_github_app"),
    ).cache()

    # 各种 count
    total_events = flagged.count()
    rule_a_events = flagged.filter("rule_a_login_suffix").count()
    rule_b_events = flagged.filter("rule_b_github_app").count()
    both_events = flagged.filter("rule_a_login_suffix AND rule_b_github_app").count()
    either_events = flagged.filter("rule_a_login_suffix OR rule_b_github_app").count()

    print(f"rule A only:    {rule_a_events:,}")
    print(f"rule B only:    {rule_b_events:,}")
    print(f"both:           {both_events:,}")
    print(f"either:         {either_events:,}")

    # Top 20 actor by event count,标记 A/B 命中
    top_actors = (
        flagged.groupBy("actor_login")
        .agg(F.count("*").alias("event_count"),
             F.max(F.col("rule_a_login_suffix").cast("int")).alias("rule_a"),
             F.max(F.col("rule_b_github_app").cast("int")).alias("rule_b"))
        .orderBy(F.col("event_count").desc())
        .limit(20)
    )
    top_actors.show(truncate=False)
```

逐段:

- `rule_b_paths` 是 5 条 JSON path——我们一开始**只 probe 了 `$.performed_via_github_app`**(payload 根部),实测 hit = 0。然后我们想:这字段真存在吗?去翻 GH Archive 原始 payload 一行行看——发现它**不在 payload 根部,在 issue / comment / pull_request / review 子对象里**。**这就是 spike 抓到的第一个真问题**。把 5 个路径全 probe 才有意义
- `rule_a_login_suffix`:朴素的字符串后缀检测
- `rule_b_github_app`:5 个 path 的 OR
- `flagged.cache()`:这个 DataFrame 后面要 count 4 次,缓存一次比每次重算快
- 输出 Top 20 actor,人工标"这个是 bot 吗"——**spike 包含人眼判断,不依赖完美算法**

## 实测结果(第一次运行,只用根 path)

```
rule A only (login ~ [bot]):     166,939
rule B only (performed_via_app): 0       ← !!!
either rule (A union B):         166,939
both rules (A intersect B):      0
```

**Rule B 一个 hit 都没有**。这就是上面说的"路径写错"。

## 实测结果(修正 Rule B path 后)

```
rule A only:     166,939
rule B only:       7,792
both:              7,752
either:          166,979
```

修正之后:Rule B 加了 40 个 event,基本可以忽略。

**Top 20 actor**(节选,完整在 [`docs/spikes/bot_heuristic.md`](../spikes/bot_heuristic.md)):

| Rank | login | events | A | B | 人工判断 |
|------|-------|-------:|---|---|----------|
| 1 | `github-actions[bot]` | 129,533 | ✅ | ✅ | bot |
| 2 | `renovate[bot]` | 6,480 | ✅ | ✅ | bot |
| 3 | `dependabot[bot]` | 6,203 | ✅ | ✅ | bot |
| 11 | `LombiqBot` | 1,752 |  |  | **bot,两条都没抓到** |
| 13 | `sonarqubecloud[bot]` | 1,274 | ✅ | ✅ | bot |

## 这个 spike 抓出来三个 finding

### Finding 1:Rule B 的 path 我们假设错了

如果不 spike,直接写 mart 用 `$.performed_via_github_app`,bot mart 里 Rule B 永远 0 hit,统计严重错。我们会在面试解释 mart 的时候摔死。

### Finding 2:Rule B 衡量的不是"actor 是 bot",而是"事件由 App 发起"

Rule B 修正路径后只比 Rule A 多 40 event 和 27 个 actor。**那 27 个 actor 几乎全是用 GitHub Mobile / Slack 集成的人类用户**——他们用 App 评论,但他们是人。

把这种 event 标成 bot 就 **过度计数**——`bot_event_share` 会偏高,业务结论就错。

**正确的处理**:Rule B 不当 bot 规则,而是当独立的"event 是否通过 App 发起" flag,跟 actor 级 bot 标识分开。这就是 [ADR-0006](../adr/0006-bot-identification.md) 里的 `is_app_event` 字段。

### Finding 3:Rule A 漏 `LombiqBot`

Top 20 里有 8 个"肉眼可辨" bot(7 个 `[bot]` 后缀 + LombiqBot)。Rule A 抓 7/8 = 87.5%。**低于我们 spike 设定的 90% 通过线**。

但失败方式有意义——漏的是命名不遵循 `[bot]` 约定的 bot。**修复方法不是放宽 Rule A 后缀(`*bot` 会误伤 `robot` `blast0rama` 等),而是加白名单**。这就是 Rule C。

## 这一步的产出

**没有产生 production 代码**(`bot_heuristic_spike.py` 是探索脚本)。但产生了三件事:

1. [`docs/spikes/bot_heuristic.md`](../spikes/bot_heuristic.md) —— 完整 finding writeup
2. ADR-0006 的方向定下:**Rule A + Rule C + event-level `is_app_event`**(代替原 plan 的 A+B)
3. Sprint 3b 的 `known_bots.csv` 第一条 entry:`LombiqBot`

## 验证 — 这一步做对了的标准

不是"代码跑通",而是:

1. 跑完 spike 我们能**清楚说出 Rule B 不该这么用**(不能、为什么不能)
2. Top 20 actor 的人工 label 表跟我们的 ADR 决策一致
3. ADR-0006 的"alternatives rejected" 段落能引用 spike 的具体数字

## 代码 review 笔记

复看 `bot_heuristic_spike.py`,有一处 senior 必看的细节:

`rule_b_expr = None` 然后在 loop 里 `rule_b_expr = probe if rule_b_expr is None else (rule_b_expr | probe)` —— 这是 Python 写 fold 的标准模式。

更"senior"的写法是用 `functools.reduce`:

```python
from functools import reduce
import operator
rule_b_expr = reduce(operator.or_,
                     (F.get_json_object("payload_raw", p).isNotNull()
                      for p in rule_b_paths))
```

更短,但**对 reader 反而不友好**。Spike 脚本一般不必追求最 idiomatic。我们留着 if/else 那版,因为它更新手友好。

## You will be able to say

### 90-second version (English)

> "Sprint 2.5 was a one-day spike before building the bot mart in
> Sprint 3. The proposed rule was: A) login ends with `[bot]`, or
> B) `payload.performed_via_github_app` is non-null. I wanted real
> data to back the assumption before committing to the mart.
>
> Three findings shifted the design:
>
> First, the field for Rule B isn't at the payload root — it's on
> sub-objects: issue, comment, pull_request, review. My first probe
> with the root path returned zero hits. I'd have shipped a broken
> mart if I hadn't measured.
>
> Second, once I fixed the paths, Rule B added only 40 events and
> 27 actors beyond Rule A. The 27 are humans using a GitHub App —
> not bots. Conflating those would inflate bot share. So Rule B got
> reframed as an event-level `is_app_event` flag, separate from
> actor-level bot classification. That's in ADR-0006.
>
> Third, Rule A catches 7 of the 8 visible bots in the top 20 by
> event count — 87.5%, below my 90% threshold. The miss is
> `LombiqBot` — a custom-named bot without the `[bot]` suffix.
> Loosening Rule A to match `*bot` would catch false positives like
> `blast0rama`. So I introduced Rule C: a curated allowlist in
> `known_bots.csv`, version-controlled, so additions go through PR
> review. The allowlist starts with one entry, LombiqBot. It can
> grow as future incidents surface."

## 常见尖刻问题 + 准备好的答案

**Q: "你 spike 1 天而不是直接 build mart,不是浪费?"**

> "Without the spike, ADR-0006 would have shipped with Rule B as
> originally framed. The mart would compute bot share with Rule B
> returning zero, silently undercounting bots. By the time someone
> notices the metric looks wrong, you're rolling back a mart that
> downstream readers already trusted. One day of spike vs. a week of
> rollback + reputation. Net positive."

**Q: "Rule C 是手工白名单,不可扩?"**

> "Yes — by design. The allowlist is small (one entry today, expected
> < 20 long-term) because most bots follow GitHub's `[bot]` convention.
> The non-convention ones are the long tail, and Rule C is
> human-curated PR-reviewed. The trade-off: scale of detection vs.
> guaranteed precision. A regex-based rule would broaden coverage
> but introduce false positives — I'd rather miss the long tail
> than overclassify a human as a bot in business metrics."

**Q: "你为什么 ADR-0006 写之前才发现 Rule B 错?"**

> "I didn't, until I ran the spike. The plan ADR draft had Rule A + B
> verbatim from what GH Archive docs imply. The spike is exactly
> the step that converts 'what docs imply' into 'what real data shows'.
> Senior engineers expect their assumptions to be wrong by default;
> they spike to find out which ones."

---

下一章 →  [06-sprint3-marts-2-and-3.md](06-sprint3-marts-2-and-3.md)
