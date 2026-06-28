# Chapter 00 — 项目鸟瞰

## 这章你会学到什么

OSS Pulse 整体是个什么东西、为什么这样切分、怎么用一句话和五句话讲清。读完这章你应该能回答面试官第一个问题:**"先用 30 秒说说这个项目是干啥的"**——并且不会瞎绕。

## 背景概念(30 秒补课)

如果你下面这些词不熟,先在脑子里建立粗略印象,后面章节会细讲:

- **GH Archive**:一个免费公开数据集,把 GitHub 上所有公开事件(push、PR、issue、star、fork 等)按小时打包成 `.json.gz` 文件,从 2011 年起每小时一个。一小时大概 100-300 MB,全年 ~2.5 TB。
- **Lakehouse**:介于 data warehouse 和 data lake 之间的架构。数据落在便宜存储(S3 / 本地磁盘)上,但用 Delta Lake 这类"table format"给它加上 ACID 事务、schema 演进、时间旅行。既能跑分析,也能流式写入。
- **Medallion architecture**:Databricks 推广的一种 lakehouse 分层模式。
  - **Bronze**:原始数据落盘后第一层,跟源数据形态接近,只做最少的修整
  - **Silver**:对 Bronze 做清洗、规范化、按业务 entity 拆开
  - **Gold**:面向业务用例的最终聚合表,被 BI / dashboard / API 直接消费
- **Delta Lake**:在 Parquet 之上加 transaction log (`_delta_log/`) 的 table format。支持 MERGE、time travel、schema enforcement。
- **dbt**:把 SQL 转换成"模型 + 依赖图 + 测试"的工具。你写 `select ...`,dbt 帮你建表、跑测试、追依赖。
- **Spark / PySpark**:分布式计算引擎。这个项目用本地单机 Spark(`master=local[*]`)模拟分布式,代码不用改就能放上 Databricks 跑大数据。
- **Airflow**:任务编排工具。把"download → ingest → run silver → run gold → test"这种有依赖关系的步骤画成 DAG,然后定时跑、失败重试、邮件告警。
- **ADR**(Architecture Decision Record):一份小文档,记录"我做了这个决定 / 我考虑过哪些替代 / 我为什么选这个 / 什么时候应该回头修这个决定"。Senior 工程师的标志,不是写了多少代码,是把不可逆决定的来龙去脉写下来。

## 项目是什么

**用一句话**:把 GH Archive 数据从 raw JSON.gz 摄入到 Delta Bronze,再用 dbt 建出 6 张 Silver 表 + 3 张 Gold mart,所有这一切被 Airflow 编排、被自定义 quality gate 守门、被 GitHub Actions CI 验证,Bronze 已经上 AWS S3,并且有个用 Redpanda + Spark Structured Streaming 的小型流式分支,流批 reconciliation 零行差。

**用五句话**(在面试里用这个):

1. OSS Pulse 是基于 GH Archive 数据集的 production-grade lakehouse,作为加拿大 Senior Data Engineer 求职 portfolio。
2. 端到端 medallion 架构:Bronze 用 PySpark + Delta MERGE 摄入,Silver / Gold 用 dbt-spark,共 11 张 Delta 表。
3. 每一层之间都有数据质量 gate,18 个 check,失败时 Airflow 就阻塞下一步。
4. Sprint 5b 我故意造了一个 schema break 触发 postmortem,记录 detection chain 在哪一层 fail,这个 drill 比单写"事故文档"有意义。
5. Sprint 6 有个 streaming MVP,把一小时 PushEvent 通过 Redpanda 流到 Structured Streaming consumer,跟批处理 Silver reconciliation 0 行差。

## 为什么是这个架构,而不是别的

下面这些选择都是有论据的,每一个对应一个 ADR(在 `docs/adr/`):

| 选择 | 别的选项 | 为什么这个 | ADR |
|------|---------|-----------|-----|
| Bronze 的 `payload` 字段存 raw JSON STRING | 完整 typed STRUCT | 上游 schema 改了,Bronze 不应该崩 | ADR-0001 |
| Bronze 用 `event_id` 做 MERGE 主键,而不是 INSERT | 用 (ingest_hour, file_offset) 之类的复合自然键 | GitHub 自己保证 event_id 全局唯一 + 不重用,613K 行实测 0 重复 | ADR-0002 |
| Bronze 按 `ingest_hour` 分区,ZORDER `created_at` | 按 `type` 分区、按 `repo_id` 分区 | 摄入时只知道 ingest_hour,这是唯一不需要额外计算就能拿到的分区列 | ADR-0003 |
| Gold 用自然 ID (`repo_id` + `activity_date`) 做 composite key | 引入 surrogate key | GitHub `repo_id` 永不改变 + 单数据源,没有引入 surrogate key 的常见理由 | ADR-0004 |
| Silver 表按需建,不一次建 15 个 | 一次把 15 个 event type 全摄入 | 不用的表是负担,每张表都是契约 | ADR-0005 |
| Bot 识别规则用 `[bot]` 后缀 + 白名单 `known_bots.csv` | 用 `performed_via_github_app` 字段 | Sprint 2.5 spike 实测,前者在 PushEvent 上 hit 多;后者衡量"事件是否经过 App",不是"actor 是不是 bot" | ADR-0006 |
| 自研轻量 DQ framework | 用完整 Great Expectations | GE 的 YAML 配置 + HTML data docs 的开销在本项目规模上不值;模仿 GE 的 checkpoint pattern,~150 行 Python 就够 | (设计 trade-off 写在 `quality/checks.py` 顶部 docstring) |

**面试官如果问"为什么用 Spark 不用 Pandas",标准答案:** "现在 Bronze 才 613K 行,Pandas 也行。但项目设计目标是回填全年 GH Archive (~2.5 TB),那个 scale Pandas 单机崩,Spark 才能横向扩。我现在 master=local[\*],代码不改就能上 Databricks 集群——这个 portability 才是选 Spark 的核心理由,而不是当前数据量需要。"

## 七个 Senior Signal

整个项目里每个 Sprint 都至少强化下面其中一项。这是项目的"北极星":

1. **Idempotency(幂等性)**:同一份 source 跑两次,不应该产生重复行。
2. **Backfill / replay**:任意时间窗口都能重跑,结果一致。
3. **Schema-drift tolerance**:上游加字段、改字段名,pipeline 不崩。
4. **DQ gates(数据质量门)**:每一层都有自动 check,失败阻断下一步。
5. **Performance tuning report**:做实验,有 before/after,有诚实结论(包括负结论)。
6. **Batch + streaming story**:流批 reconciliation,验证两条路径产物一致。
7. **Operational docs**:ADR + runbook + postmortem,让"为什么这么做"可追溯。

读 walkthrough 时,每章读到一半你应该能说出"这章主要强化第几号信号"。

## 接下来读什么

按这个顺序:

- **Ch 01** 讲 Sprint 0(schema 探索)——为什么我们不直接动代码,而先花一周读数据
- **Ch 02** 讲 Sprint 1 Bronze——idempotency 和 partition 怎么落地
- **Ch 03** 一路接到 Gold

读到 **Ch 09**(postmortem)和 **Ch 12**(面试存活)是最有 senior signal 的两章——如果时间紧,跳到这两章看也行。

## You will be able to say

### 30-second elevator pitch (English)

> "OSS Pulse is a portfolio lakehouse on the GH Archive dataset.
> Bronze, Silver, Gold via PySpark + Delta + dbt, with a parameterized
> Airflow DAG and 18 data-quality gates at every layer boundary. It
> reinforces what I call seven senior signals — idempotency, backfill,
> schema-drift tolerance, DQ gates, perf tuning, batch-streaming
> reconcile, and operational docs. Sprint 5a got the Bronze layer
> live on AWS S3, and Sprint 6 has a streaming MVP that reconciled
> 181 thousand events against batch with zero row delta. The
> highest-signal piece is Sprint 5b, where I injected a schema break
> on purpose and wrote up a postmortem on which gate caught it,
> which one leaked, and why gate-placement matters more than gate
> count."

### Two-minute version (English)

> "I'll go layer by layer. Bronze stores GitHub events as raw JSON
> strings — that's ADR-0001, schema-drift containment. Silver
> parses per event type, only the types my Gold marts need, ADR-0005.
> Gold has three marts: repo daily activity, OSS health, and bot
> vs human. Each layer MERGEs on the GitHub event id, so re-running
> any partition gives the same result — ADR-0002 idempotency.
>
> Quality gates run between every layer. I deliberately wrote a
> small framework instead of pulling Great Expectations — the
> design trade-off is in `quality/checks.py`'s docstring. The
> gates caught a real cross-mart inconsistency in Sprint 3 where
> two Gold marts disagreed on bot classification, and that drove
> ADR-0006 to centralize the bot rule in a dbt macro.
>
> The two most senior pieces: Sprint 5b's deliberate-incident
> drill, where I observed that detection happens at end-of-pipeline
> instead of between Silver and Gold — gate-placement, not gate
> count, is the real lesson. And Sprint 6's streaming MVP that
> reconciled batch and streaming row-by-row to zero delta on a
> hundred-eighty-one-thousand event sample, exactly-once via
> foreachBatch plus Delta MERGE."

## 常见尖刻问题 + 准备好的答案

**Q1: "这个项目你做了多久?"**

诚实答:"part-time 八到十周。原计划六周,后来发现 Sprint 4 + Sprint 5b 工作量被低估,延长到十周,这个估计偏差也写在 `PROJECT_PLAN.md` 里。"

**(为什么这答有效)** 面试官想看你估时间的能力 + 修正估计的能力,**不是**想听"我一周搞完了"。承认低估更可信。

**Q2: "为什么是 portfolio 而不是工作项目?"**

"我现阶段在转 Senior DE 方向,prod 经验主要在 [你之前的角色]。这个项目是补 portfolio 维度——不能替代真生产 scars,但 senior signal 是我刻意设计的。"

**Q3: "这数据其实不大,真 prod 你怎么扩?"**

"现在 Bronze 4 个 ingest_hour,613K 行。设计目标是全年回填 ~2.5 TB,partition by ingest_hour + ZORDER by created_at 的方案在 Databricks 上每天加一个分区就能横向扩。我在 Sprint 5b 性能调优里用 4 个 partition 实测过 ZORDER 在小规模上的表现,**它实际上没起作用**——这个负结论本身就是我对 scale 的诚实理解,而不是嘴上说说我'懂'。"

(这一答把"项目规模小"这个负点反手变成"我已经诚实测过 perf tuning 的边界")

---

下一章 →  [01-sprint0-schema-discovery.md](01-sprint0-schema-discovery.md)
