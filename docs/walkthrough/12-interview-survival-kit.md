# Chapter 12 — 面试存活包

## 这章你会学到什么

把前面 11 章学到的拼成可以**真正在面试里讲出来的结构化输出**。读完你能:**5 分钟、15 分钟、45 分钟三个版本对症下药地讲项目;遇到 10 类常见尖刻问题准备好答案;遇到"这部分缺啥" 知道怎么诚实回答**。

## 关联前后

- **上一章** ([Ch 11](11-sprint5a-cloud-migration.md)) cloud 收尾
- **这是最后一章**——之前都是 "怎么做" 的知识,这一章是 "怎么讲"

## 5 分钟版本(电梯里 / phone screen 开场)

### 0:00-0:30 What it is

> "OSS Pulse is a portfolio lakehouse on the GH Archive dataset —
> end-to-end medallion architecture on PySpark, Delta Lake, dbt,
> and Airflow, with a parameterized Airflow DAG, 18 data-quality
> gates, and a streaming MVP that reconciled batch and streaming
> with zero row delta on 181,000 events. The Bronze layer is
> live on AWS S3 via Terraform."

### 0:30-1:30 Architecture in three sentences

> "Bronze stores 613K GitHub events with the payload as raw JSON
> STRING — ADR-0001, so upstream schema drift never crashes
> ingestion. Silver builds per-event-type tables only when a Gold
> mart needs them — ADR-0005, demand-driven. Gold has three marts:
> repo_daily_activity, oss_health_mart, and bot_vs_human_activity_mart.
> Every layer MERGEs on the GitHub event id — that's the idempotency
> chain from ADR-0002."

### 1:30-3:00 The two highest-signal pieces

> "Two things I'd want a Senior DE interviewer to walk away
> remembering. First, Sprint 5b's deliberate incident drill — I
> renamed payload.size to payload.commit_count on 200 synthetic
> rows, ingested them, and observed every gate in the pipeline.
> Bronze ingest passed, Bronze gate passed, Silver build passed,
> Silver gate passed. The first detection was dbt test at
> end-of-pipeline. In production, Gold would already be polluted.
> The lesson — gate-placement matters more than gate count — drove
> a new regression check at the Silver gate so the next same-shape
> incident is caught between Silver and Gold.
>
> Second, the cross-mart verifier in Sprint 3b. I wrote
> gold_bot_verify.py to check that repo_daily_activity's
> bot_push_count matches bot_vs_human_activity_mart's push_bot_count
> for every joined repo-day. First run after Sprint 3b: 108
> mismatches. Two marts were using different bot rules — Sprint 2's
> inline `like '%[bot]'` versus Sprint 3b's is_bot() macro
> including a known_bots allowlist. The verifier I wrote caught my
> own inconsistency and forced me to centralize the bot rule
> through a dbt macro. That kind of self-policing is the difference
> between 'I shipped two marts' and 'I shipped two marts I can
> prove agree'."

### 3:00-4:30 What's real, what's a portfolio limit

> "The honest scoring: this is a strong Senior signal *combined
> with real production experience*. As a pure portfolio with no
> backing production experience, it's strong intermediate plus. I
> know the gaps — pytest coverage is light (3 Python tests, mostly
> dbt schema tests), CI doesn't run dbt build (deferred to a
> warehouse-connected workflow), and the cloud migration finished
> the Bronze layer but Databricks compute is pending the user's
> Free Edition signup. Each of those gaps has a documented
> remediation path. The work itself — schema discovery, ADRs,
> postmortem drills, cross-mart verifiers — is the senior signal."

### 4:30-5:00 Close

> "Repo's at github.com/MistFall-Wang/oss-pulse, with a visual
> showcase site, a 12-chapter walkthrough, and a 5-minute video
> demo script. Happy to dive into any specific layer."

---

## 15 分钟版本(技术深度面试,有时间一层一层讲)

下面按 **5 个 90-second segments** 组织,加 60-second 缓冲。

### Segment 1: Context + 7 senior signals(90s)

引用 Ch 00 的 elevator pitch + 7 个 signal 列表。结尾:**"我会按 Bronze → Silver → Gold → DQ → 流式 → 上云 → postmortem 的顺序讲,你可以在任何一段打断。"** 主动给 interviewer 跳出权。

### Segment 2: Bronze + idempotency 故事(90s)

引用 Ch 02 的 2-min 版,着重讲:

- 用 text() 不用 json()——掌控 type inference
- MERGE on event_id,无 UPDATE 分支
- `count(*) == count(distinct id)` 每次 ingest 后跑

**Trigger 跳到 ADR-0002 故事**:

> "And the reason I trust event_id as the key — three years of
> sample with zero duplicates, plus GitHub's API contract that
> ids are immutable. ADR-0002 has the evidence."

### Segment 3: Silver + dbt 故事(90s)

引用 Ch 03 的 2-min 版。重点:

- `on_schema_change='fail'`(explicit > implicit)
- `delta_source` macro
- 9 个 declarative test

### Segment 4: Gold + composite key + cross-mart(120s,稍长一点)

引用 Ch 04 + Ch 06 的合集。

**关键节奏**:
- "First Gold mart in Sprint 2 — composite key over surrogate, ADR-0004."(30s 解释 4 个 Kimball 动机为什么不成立)
- "Sprint 3 expanded — five more silver tables, two more marts."(30s)
- **"The interesting moment was the cross-mart verifier"** —— Ch 06 的 LombiqBot 故事(60s)

### Segment 5: Postmortem + streaming(120s,最大单段)

直接背 Ch 09 的 3-min 版,稍简。

If 时间充足,接上 Ch 10 streaming reconcile,4 行收尾:
- Redpanda over Kafka
- foreachBatch + MERGE = exactly-once
- Three-layer reconcile (row count + commits sum + set-difference)
- 0 row delta on 181K

### Segment 6: Cloud + honest evaluation(90s)

Ch 11 的 3-min 版前半 + 自我承认:

> "Cloud step is partial — I did the S3 piece end to end (Terraform,
> aws s3 sync, smoke test reading from S3). Databricks compute is
> pending the Free Edition signup. Step 9.0 in my walkthrough has
> the simplified-apply runbook for users with limited IAM, which
> is the situation I hit myself."

### Segment 7: 5-second close + invite question

> "That's the tour. Happy to deep-dive on any sprint or any ADR."

---

## 45 分钟版本(deep-dive coding interview / system design 风格)

不再是 elevator pitch,而是**让面试官引导,你按章节展开**。

策略:**让面试官选起点**。

> "Hey, the project has 13 sprints and 7 ADRs. Would you like to
> start with architecture, with the deliberate-incident drill, or
> with a specific layer's code?"

这一句话让 interviewer 觉得 control 在他们手上。**然后他们一定选 incident drill 或 streaming**(最 senior 的 talking points)。

无论他们选啥,你的回应模板:

1. **30s 概括**(直接背 Ch X 的 1-min version)
2. **打开实际文件分享屏幕**——准备好 README + Ch X + 对应 code file
3. **从 file 一段一段 walk through**,引用 ADR
4. **过一会儿,主动 prompt**:"want me to dive into [related sprint]?" — 控制节奏

时间表:

| 起点 | 30s 概括 → | 5 min file walk → | 10 min 相关 sprint → | 5 min ADR / trade-offs |
|------|-----------|------------------|---------------------|----------------------|
| Architecture | Ch 00 elevator | README → diagram | Bronze + Silver | ADR-0001, 0005 |
| Incident drill | Ch 09 1-min | incident_inject.py + postmortem | Sprint 4 gate design | ADR-0006, gate-placement lesson |
| Streaming | Ch 10 2-min | consumer.py | Sprint 6 reconcile + cross-mart | exactly-once vs at-least-once |
| Cloud | Ch 11 3-min | terraform/s3.tf + s3_smoke_test.py | IAM/KMS fallback 故事 | least-privilege trade-off |

---

## 10 类常见尖刻问题 + 准备答案

### Q1:"这数据规模太小,你怎么知道你的设计能 scale?"

**坏答**:"It would scale because Spark is distributed."

**好答**:

> "It scales by architecture, not by current data volume. Partition
> by ingest_hour means adding partitions horizontally — one new
> partition per hour. ADR-0003's revisit clause specifies the
> threshold: if Bronze grows past ~100 ingest_hours and per-type
> Silver build becomes a bottleneck, partition by event type also
> becomes worth the complexity. Right now it's not, because at 4
> partitions data skipping doesn't fire — Sprint 5b's perf
> experiment proved that empirically. Scale is a function of when
> you measure, not what you claim."

### Q2:"你这都是 portfolio,怎么证明你能在 prod 里工作?"

> "I can't prove that with code alone. What I can prove is the
> discipline pieces — postmortem-driven gate placement, cross-mart
> consistency verifiers, honest negative perf results, ADRs that
> document why decisions were made and when to revisit. Those are
> the habits from prod work mapped onto a portfolio. My actual
> prod experience is at [previous role / context]; this project
> is the portable evidence."

### Q3:"为什么用 Spark 不用 Pandas?"

> "Pandas works at the current 613K rows. The architecture target
> is GH Archive full-year backfill, ~2.5 TB, which Pandas can't
> single-node. Spark on local[*] runs the same code that runs on
> Databricks at cluster scale — that portability is the reason,
> not the current data volume. If the project's target were just
> the current sample, Pandas would be the right choice."

### Q4:"dbt-spark 上 prod 你怎么做?"

> "Sprint 5a's plan is to swap dbt-spark for dbt-databricks. ADR-0005
> has the audit list — register_external_sources macro needs S3
> path handling, MERGE syntax difference between adapters, schema
> name generation. The local dev target stays on dbt-spark for
> offline work; prod target uses dbt-databricks against a Databricks
> SQL warehouse. The cloud_migration runbook has the full step-by-step."

### Q5:"你 ADR 写这么多,真的会回头读吗?"

> "Two cases I did, both documented in the project. ADR-0006 was
> rewritten after Sprint 2.5's spike — original draft had Rule B
> which the data disproved. ADR-0009 (VACUUM cadence) was promoted
> from optional Sprint 9 to mandatory because Sprint 5b's perf
> experiment showed OPTIMIZE temporarily doubles Bronze storage.
> In both cases, ADRs were the place that recorded what evidence
> changed the decision. Without them, the lessons would have lived
> only in git commits no one reads."

### Q6:"你 GE 都没用,怎么算 production-grade?"

> "I evaluated GE — set up a DataContext, defined a few
> expectations, generated HTML docs. For this project's scale,
> the YAML config and HTML overhead exceeded the value. I wrote
> 150 lines of Python that mirror GE's checkpoint pattern — same
> substance: declarative checks, pass-or-fail with detail,
> exit-code gating. The docstring in checks.py spells out the
> trade-off. If a role requires hands-on GE, porting these checks
> is a one-day exercise — the abstraction is already aligned."

### Q7:"你做的 incident postmortem 是假的(因为你自己造的),真生产事故跟这个一样吗?"

> "It's a controlled experiment, not a real incident — and I
> labeled it that way in the postmortem. But the value isn't the
> incident; it's the detection chain analysis. The same drill
> done in real prod would have produced the same five-Whys output
> and the same gate-placement insight. What you can't simulate in
> portfolio is the on-call pressure, the customer-visible impact,
> the multi-team coordination. I'd describe this work as 'incident
> drill', not 'postmortem of a real production incident'. The two
> are different — the latter requires real prod scars."

### Q8:"你这个有 multi-source reconciliation 吗?"

> "No — single-source. GH Archive only. ADR-0004 has a revisit
> clause for multi-source — that would force introducing surrogate
> keys and source-prefix tracking. If a future GitLab + GitHub
> reconciliation is the goal, the work is documented and not
> hidden. The current scope is one source done well, rather than
> two sources done partly."

### Q9:"你这是不是只在 Mac 上跑过?Linux 怎么样?"

> "CI runs on ubuntu-latest with JDK 17 — that exercises the Linux
> Spark path on every PR. The Mac-specific things are JAVA_HOME
> defaults in scripts, which are overridable via OSS_PULSE_JAVA_HOME
> env var. The walkthrough docs note this in the 'common failures'
> table. A Linux clone needs `OSS_PULSE_JAVA_HOME=/path/to/jdk17`
> set; that's it."

### Q10:"如果你重做一遍,会改什么?"

> "Three things in priority order. First, more pytest — 3 Python
> tests is the weakest dimension. I'd write 12-15 more covering
> quality/ checks, streaming reconcile, and Airflow DAG validation.
> Second, CI should run dbt build against an embedded warehouse,
> not just dbt parse + compile. Third, the deliberate-incident
> drill is one shape of failure — I'd add 2-3 more (type
> narrowing, field deletion, type set change) to prove the
> detection chain across multiple drift modes. Each is ~2 hours,
> would push the project from 8/10 to maybe 8.5/10 — but
> wouldn't change the seniority signal materially."

---

## "这个你怎么没做" 的诚实回答

如果 interviewer 找到 gap,**不要假装做了**,**不要急着补一句"但是我可以做"**。

模板:

> "Right — [X] is not in the project. I considered it and chose
> not to ship it because [Y]. The documented path to add it is in
> [Z]. It's a real gap, not an oversight."

具体应用:

- "你没做 Databricks 上 prod 跑通"
  > "Right — Databricks Free Edition compute is pending my own
  > workspace sign-up. Bronze layer is live on S3 via Terraform,
  > smoke test verified, but dbt build --target prod hasn't run
  > yet. The cloud_apply_walkthrough.md covers steps 7-11 with
  > exact commands when I do sign up."

- "你没做 unity catalog / governance"
  > "Right — single-developer project, no governance layer.
  > Sprint 5a's plan would set up Unity Catalog as part of the
  > Databricks workspace bring-up. Not in current scope."

- "你没做 schema registry / Avro"
  > "Right — payload is raw JSON, not schema-registry'd. That's
  > ADR-0001's deliberate choice — schema-drift containment over
  > schema-enforcement. If the use case shifted to streaming
  > heavy with stable schemas, Avro + schema registry would be
  > the alternative; ADR-0001 has the trade-off documented."

承认局限本身就是 senior signal。**junior 慌忙找借口,senior 平静承认 + 说出 trade-off**。

---

## 5 个"绝不说"的句子

下面这些话**杀掉所有 senior signal**。不管多紧张都不要说:

1. ❌ "I think this would scale because Spark is distributed."
   → 改成具体 partition / sharding 论据
2. ❌ "I used Delta because Iceberg is too complex."
   → 改成"I considered Iceberg's hidden partitioning and time travel; chose Delta because of the dbt-spark adapter maturity and MERGE syntax. ADR doesn't exist for this because both work; Iceberg would be the alternative if Unity Catalog requires it."
3. ❌ "I followed best practice X."
   → "Best practice" 是空话。**改成"I chose X because Y, and the trade-off is Z."**
4. ❌ "It works on my machine."
   → 改成"I haven't verified on Linux at the OS level; CI runs ubuntu-latest with JDK 17, exercises that path on every PR."
5. ❌ "I'll add tests later."
   → 改成"Test coverage is thin — 3 Python tests, mostly dbt schema. I deliberately prioritized dbt tests + cross-layer verifiers over Python unit tests for this project; the gap is documented and I'd write 12-15 more in 2-3 hours of focused work to push the testing dimension from 7/10 to 8.5/10."

---

## 最后:Calibration self-check

每次模拟讲完,检查以下 5 条。

| 检查 | 标准 |
|------|------|
| 我有没有说"best practice"? | 0 次 |
| 我有没有引用具体数字? | 至少 3 个具体数字(613K, 0 row delta, 87.5% recall…) |
| 我有没有承认局限? | 至少 1 个 explicit gap + remediation path |
| 我有没有讲一个具体决策的 trade-off? | 至少 1 个明确的 "I chose A over B because…" |
| 我有没有给 interviewer 提问的钩子? | 结尾 "happy to dive into ..." |

5 个都过 = 你 ready 了。

---

## Walkthrough 全局回顾

| 章 | 关键 takeaway |
|---|--------------|
| [00](00-overview.md) | 7 senior signal + elevator pitch |
| [01](01-sprint0-schema-discovery.md) | "evidence-driven design" 比 "best-practice design" 强 |
| [02](02-sprint1-bronze-ingestion.md) | MERGE 让幂等性成为表的属性 |
| [03](03-sprint1-first-silver.md) | dbt 的真价值是 ref/source/test 三位一体 |
| [04](04-sprint2-first-gold-mart.md) | composite key 在单数据源里 strictly better than surrogate |
| [05](05-sprint2.5-bot-spike.md) | spike 用一天的成本换 mart 一周的返工 |
| [06](06-sprint3-marts-2-and-3.md) | 用 macro 做 single source of truth,verifier 自己抓 drift |
| [07](07-sprint4-dq-airflow-runbooks.md) | gate 放 build 之间,不是 build 之后 |
| [08](08-sprint5b-ci-and-perf.md) | 负实验结果是 senior signal,不是失败 |
| [09](09-sprint5b-incident-postmortem.md) | gate-placement > gate count;每个 postmortem 留下 regression check |
| [10](10-sprint6-streaming-mvp.md) | exactly-once 是结果的属性,不是 delivery 的属性 |
| [11](11-sprint5a-cloud-migration.md) | least-privilege 撞墙时 graceful degrade 写进 runbook |
| [12](12-interview-survival-kit.md) | 这章本身——3 个 version + 10 个 Q&A + 5 个绝不说 |

---

读完这 13 章 + 几次自我复述 + 录一次 5 分钟 video demo = **你可以独立 own 这个项目讲给任何 hiring manager**。

祝面试好运。

---

[← README](README.md)
