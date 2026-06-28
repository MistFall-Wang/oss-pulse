# Chapter 01 — Sprint 0:Schema 探索

## 这章你会学到什么

为什么 senior DE 不会一上来就动代码,而是先花时间读数据、找形状。读完这章,你能解释:**为什么 Bronze 的 `payload` 字段最终决定存成 raw JSON STRING 而不是 typed STRUCT**,并且这个决定有具体数据撑腰。

## 关联前后

- **上一章** ([Ch 00](00-overview.md)) 给了项目鸟瞰
- **下一章** ([Ch 02](02-sprint1-bronze-ingestion.md)) 用本章得出的结论真正写 Bronze 摄入

## 背景概念(30 秒补课)

- **Schema discovery / data profiling**:在动手写 pipeline 前先观察数据。看每个字段长什么样、有没有 null、值的分布、嵌套层级多深。Senior 不省这一步,junior 跳过这一步就开始写 bug。
- **Envelope**:JSON 数据里"顶层那一圈字段",在 GH Archive 里就是 `{"id", "type", "actor", "repo", "payload", "public", "created_at", "org"}` 这 8 个。
- **Schema drift**:上游(GitHub)隔几年加一个字段、改一个字段名、加一个新的 event type。下游 pipeline 不能因此崩。
- **typed STRUCT vs raw STRING**:Spark 表里 JSON 数据可以存两种方式——一是定义完整 schema 解析成嵌套 STRUCT(每个字段有类型),二是直接当 STRING 存,需要时再 `get_json_object()`。前者读取快但 schema 一变就崩,后者灵活但每次解析有开销。

## 这一阶段的目标

**问题**:GH Archive 数据从 2011 年开始,每年的 schema 都在变。我们要存哪些字段?用什么类型?

**目标**:不写一行 Bronze 代码,先用真实数据回答 7 个设计问题:

1. envelope 字段的形状稳定吗?
2. 跨 10 年(2015 / 2018 / 2025)envelope 有没有变?
3. payload 的嵌套深度有多深?跨年增长多少?
4. event type 集合稳定吗?
5. event_id 真的全局唯一吗(为我们的 idempotency 决策铺路)?
6. `public` 字段真的永远是 true 吗(GH Archive 文档说是,我们要实测)?
7. `created_at` 时间戳格式跨年是否一致?

## 代码逐行讲 — Sprint 0 deliverable

Sprint 0 产出三份文件:

- **[`notebooks/01_schema_discovery.py`](../../notebooks/01_schema_discovery.py)** — 用 Spark 读三年的 sample,回答上面 7 个问题
- **[`docs/schema_discovery.md`](../schema_discovery.md)** — 把 notebook 的发现写成可读文档
- **[`docs/schema_drift_evidence.md`](../schema_drift_evidence.md)** — 跨年对比表
- **[`docs/adr/0001-payload-handling.md`](../adr/0001-payload-handling.md)** — 基于上述发现的设计决策

### Notebook 在做什么

打开 `notebooks/01_schema_discovery.py`,代码骨架是:

```python
# 1. 拉三个年份的代表样本
years = ["2015-01-15-12", "2018-01-15-12", "2025-01-15-12"]
# (这三个时间点都是工作日中午 UTC,流量比较有代表性)

# 2. 对每年读取并 infer schema
for year_hour in years:
    df = spark.read.json(f"data/raw/{year_hour}.json.gz")
    df.printSchema()   # 看整个 schema 树
    ...

# 3. 对 payload 子树 explode,数嵌套路径数
def count_payload_paths(df, event_type):
    paths = ...  # 递归遍历 nested struct
    return len(paths)

# 4. 对每个 event_type 算"嵌套路径总数"
# 5. 跨年对比同一 event_type 的路径数变化
```

**这一步不是为了写出能跑 prod 的代码**,而是回答问题。Notebook 是探索工具。所以代码风格可以散一点、print 多一点,跟生产代码标准不同。

### 关键发现 — 写在 docs/schema_discovery.md 里

实际跑出来的数据(摘录,完整在那个 md 里):

| 字段 | 类型 | 跨年(2015/2018/2025) | 结论 |
|------|-----|---------------------|------|
| `id` | string | 三年都存在,类型一致 | 强类型化 |
| `type` | string | 三年都存在,值集从 11 → 14 → 15 个 | 强类型化但要容忍新值 |
| `actor.id` | long | 三年一致 | 强类型化 |
| `repo.id` | long | 三年一致 | 强类型化 |
| `org` | nullable object | 跨年 nullable 一致 | 强类型化 + nullable |
| `created_at` | ISO 8601 string | 三年格式一致 | 强类型化 |
| `public` | boolean | 三年都是 `true`(GH Archive 只收集 public 事件) | 强类型化 |
| `payload` | wildly different | 嵌套路径数 IssueCommentEvent 2015:**120** → 2025:**309** | **不能强类型化** |

**两个关键数字**:

- **`IssueCommentEvent.payload` 的嵌套路径从 120 涨到 309**,十年间字段增长了 2.5 倍。如果当初定义了完整 STRUCT,Sprint 1 的代码就崩了。
- **`PullRequestReviewEvent` 在 2025 sample 出现,2015 sample 不存在**。新增 event type 是常态。

### docs/schema_drift_evidence.md 长什么样

这份文档把跨年 schema 对比成 markdown 表,reviewer 一打开就能看到证据,不用自己跑 Spark。例子:

```text
| event_type        | 2015 nested paths | 2025 nested paths | drift  |
|-------------------|------------------:|------------------:|-------:|
| PushEvent         | 14                | 15                | +1     |
| IssueCommentEvent | 120               | 309               | +189   |
| PullRequestEvent  | 71                | 178               | +107   |
```

这种"用真实数据反驳设计假设"的文档是 senior signal。Junior 写 README 喜欢说"我们用 Delta 因为它好",senior 写 ADR 喜欢说"我考虑了 Iceberg 和 Delta,跑了下面三个 benchmark,选 Delta 是因为这三组数字"。

## ADR-0001 怎么写出来的

[`docs/adr/0001-payload-handling.md`](../adr/0001-payload-handling.md) 用的是 MADR-lite 模板,核心七段:

1. **Status**:Accepted(已采纳;另一个选项是 Proposed/Superseded)
2. **Context**:为什么这个决定要做。直接引用 schema_drift_evidence 里那张表。
3. **Decision**:Bronze 存 `payload_raw` (STRING) + 可选的窄小 `payload_probe` (STRUCT),只有 5 个一级字段。
4. **Consequences**:好处(schema drift 永不崩)+ 代价(每次 Silver 解析有 `get_json_object` 开销)。
5. **Alternatives rejected**:为什么不存 typed STRUCT(payload_paths 数据撑腰)。
6. **Status conditions for revisit**:什么时候应该重审这个决定(如果某个 Silver 表每天解析 1B 行,性能瓶颈在 JSON 解析时)。
7. **References**:链接到证据文件。

**面试官如果问"你怎么决定 payload 存 STRING 而不是 STRUCT 的"**,你的答案不是"因为 STRING 更灵活",而是:

> "I sampled three years of GH Archive — 2015, 2018, 2025 —
> and counted nested JSON paths per event type. IssueCommentEvent's
> payload grew from 120 paths to 309 paths in a decade. If I'd
> typed it at Bronze, the table would have crashed every year an
> upstream field was added. So Bronze keeps payload as raw JSON
> STRING and Silver parses on demand. That's ADR-0001."

这答案的特征:**有具体数字、有时间窗、有替代方案明确被否决**。

## 验证 — 这阶段怎么知道做对了

Sprint 0 的"做对"标准不是代码跑通(都没写代码),而是:

1. 三份文档都写完了
2. ADR-0001 被另一个 senior(或你自己一周后)读完能 challenge 不出来
3. 后面所有 Sprint 真的没因为 schema drift 崩过——这一点要等 Sprint 5b 的 incident drill(我们故意造一次 schema break,看 Bronze 是不是真的扛住)

## 代码 review 笔记

(我做这章时复看了 Sprint 0 的产物,有一处可以补强)

**问题**:`notebooks/01_schema_discovery.py` 现在用 `spark.read.json` 自动 infer schema,这种方式在小样本(2015 sample 只有 21K 行)上有时会把 long 字段 infer 成 string。如果当初没人留意,Bronze schema 也跟着错。

**修复**:Sprint 1 的 `spark/schemas.py` 显式定义 envelope schema,**不依赖** infer。这是一个隐含修复,但 walkthrough 应该把这个对比讲出来,你面试时能讲。下面我就在 Ch 02 里把这一点讲清。

## You will be able to say

### 1-minute version (English)

> "Sprint 0 was schema discovery — no pipeline code yet. I sampled
> three years of GH Archive, 2015, 2018, and 2025, on the same UTC
> hour. The top-level envelope was stable across all three years —
> id, type, actor, repo, org, payload, public, created_at — same
> field names, same types. So I could strongly type the envelope.
> But the payload sub-structures grew dramatically. IssueCommentEvent
> went from 120 nested paths to 309 over the decade, and
> PullRequestReviewEvent didn't exist in 2015 but did in 2025.
> That's why ADR-0001 stores payload as raw JSON STRING — Bronze
> never crashes on a new payload field. Silver parses on demand
> only for the event types my Gold marts need."

### Why-it-matters version

> "The senior signal here isn't picking Delta over Iceberg. It's
> refusing to write Bronze code for one week and instead measuring
> the data first. That measurement produced the actual number —
> 120 to 309 paths — that anyone can verify. That's the difference
> between 'I think payload should be raw' and 'I have evidence
> payload must be raw'."

## 常见尖刻问题 + 准备好的答案

**Q: "为什么不直接全表 schema enforcement?"**

> "Schema enforcement at Bronze fails the moment upstream changes —
> and upstream changes annually based on my drift evidence.
> Enforcement at Silver, where I parse per-event-type for known
> Gold consumers, is the right placement. Bronze's job is durability,
> not typing."

**Q: "为什么你没用 schemaInference + mergeSchema?"**

> "mergeSchema works for known-shape evolution but silently widens
> types — int → string when a row violates. That's a worse failure
> mode than 'parse fails loudly' for an OLAP pipeline. I want
> Silver to fail fast if payload shape changes; mergeSchema would
> hide it."

**Q: "你 Sprint 0 花了一周是不是太长?"**

> "It was 3-4 evenings actually, not a full week. But the work
> output prevented every later Sprint from having to debug 'why
> doesn't this Silver column have data'. The 9-line ADR is
> evidence I considered alternatives, not just picked one."

---

下一章 →  [02-sprint1-bronze-ingestion.md](02-sprint1-bronze-ingestion.md)
