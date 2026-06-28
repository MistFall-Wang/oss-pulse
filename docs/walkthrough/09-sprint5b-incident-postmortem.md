# Chapter 09 — Sprint 5b 下半:故意制造事故 + Postmortem

## 这章你会学到什么

为什么 senior 会**故意**给自己造一次事故,以及怎么把这个事故写成最有 senior signal 的 portfolio 章节。读完你能解释:**detection chain 是什么、为什么"gate 放在哪一层"比"gate 写多少个"更重要、5 Whys 怎么写不流于形式、为什么每个 postmortem 必须留下一个 regression check**。

这一章是整个项目里 senior signal 最强的部分。**面试时如果只有 3 分钟讲一段,讲这章**。

## 关联前后

- **上一章** ([Ch 08](08-sprint5b-ci-and-perf.md)) 完成 CI + perf 实验
- **下一章** ([Ch 10](10-sprint6-streaming-mvp.md)) 进入 Sprint 6 streaming MVP

## 背景概念(30 秒补课)

- **Postmortem**:事故复盘文档。包含:发生了什么、怎么 detect 到、怎么 mitigate、root cause、应该改什么。在好公司里所有 sev1/sev2 事故都必写。
- **5 Whys**:连问 5 次"为什么"找到 root cause 的技术。表面 cause 不是 root cause,要刨到机制。
- **Detection chain**:数据流过 pipeline 的每一步("ingest → gate → silver → gate → gold → test"),记录每一步对一个 bad input 的反应。
- **Schema-drift**:上游数据格式变了——加字段、删字段、改名、改类型。pipeline 必须 graceful 处理。
- **Regression check**:postmortem 后加的一个 check,确保下次同样 root cause 的事件被早期 detect。"每个 postmortem 至少留下一个 regression check"是好工程的硬规则。

## 这一阶段的目标

**理论上**,我们的 pipeline 该有 schema-drift tolerance(ADR-0001)。但**我们没真实验过**——portfolio 数据稳定,没人改字段。

**Sprint 5b 下半的设计**:**主动造一次 schema break**。具体说,GH Archive 里 PushEvent 的 `payload.size` 字段,我们把它在合成数据里改名成 `payload.commit_count`,然后摄入。然后**观察 pipeline 每一层的反应**——哪一层最先 fail?哪一层 silent corrupt?

这种 drill 真生产团队也做(叫 chaos engineering / fault injection)。portfolio 里做出来,是 senior 罕见亮点。

## 设计决策怎么做的

### 决策 1:Inject 到哪一层

候选:

- (a) **改 Bronze 的 `payload_raw` 列直接 update**
- (b) **写一个 synthetic source file,通过正常的 `bronze_ingest.py` 摄入,只是这个 file 的 payload 有 `commit_count` 代替 `size`**
- (c) **改上游 source URL**

选 (b)。理由:

- (a) 改 Bronze 直接 update 不真实——绕过了正常 ingest path,没法测 Bronze 的 schema-drift 容忍
- (c) 改 source URL 没法控
- (b) 走正常 ingest path,真实模拟"GH Archive 改了字段名"

### 决策 2:Inject 多少行

- 太少(10 行):统计上看不出差异
- 太多(10万行):污染数据,清理代价大

选 200 行——足够 dbt test fail 时看见明显数字,清理 30 秒。

### 决策 3:用什么 ingest_hour 标识 incident 数据

不能用真实 hour(会跟真数据混)。选 `2099-12-31-23`(将来不可能到达的时间)。**这样 cleanup 时 `delete from bronze where ingest_hour = '2099-12-31-23'` 就一行 SQL 清干净**。

## 代码逐行讲 — `spark/jobs/incident_inject.py`

```python
INCIDENT_HOUR = "2099-12-31-23"
INCIDENT_FILE = f"/tmp/incident_{INCIDENT_HOUR}.json.gz"
BRONZE_PATH = "data/bronze/events"
SOURCE_TEMPLATE = "data/raw/2025-01-15-12.json.gz"


def synthesize_incident_file(n_rows: int = 200) -> None:
    """Take first n_rows PushEvents from 2025-01-15-12 source,
    rename payload.size → payload.commit_count, write to /tmp."""
    out_lines = []
    seen = 0
    base_id = 999_000_000_000   # synthetic id space, well above real GitHub ids
    with gzip.open(SOURCE_TEMPLATE, "rt") as src:
        for line in src:
            event = json.loads(line)
            if event.get("type") != "PushEvent":
                continue
            payload = event["payload"]
            if "size" not in payload:
                continue
            # The breaking change:
            payload["commit_count"] = payload.pop("size")
            event["id"] = str(base_id + seen)
            event["created_at"] = "2099-12-31T23:00:00Z"
            out_lines.append(json.dumps(event))
            seen += 1
            if seen >= n_rows:
                break

    with gzip.open(INCIDENT_FILE, "wt") as out:
        for line in out_lines:
            out.write(line + "\n")
    print(f"[inject] wrote {seen} broken PushEvent rows to {INCIDENT_FILE}")
```

逐段:

- 读真实 source 文件作为模板,过滤出 PushEvent 行
- 关键一行:`payload["commit_count"] = payload.pop("size")` —— 改字段名
- 把 `id` 换成 999 开头的合成区段,避免跟真 event id 撞
- `created_at` 标 2099-12-31,跟 incident_hour 对应,**这样 Silver 的 `cast(created_at as date)` 会得到 `2099-12-31`,Gold mart 不会跟真数据混**

然后 main() 用正常 `bronze_ingest` 摄入这个 file:

```python
def main():
    import sys
    if "--cleanup" in sys.argv:
        # ... clean up incident partition ...
        return

    synthesize_incident_file()

    res = subprocess.run([
        ".venv/bin/python", "-m", "spark.jobs.bronze_ingest",
        "--source", INCIDENT_FILE, "--bronze-path", BRONZE_PATH,
    ], env={
        "JAVA_HOME": "/Library/Java/...",
        "PATH": "...",
        "HOME": os.environ.get("HOME", ""),
    }, capture_output=True, text=True)

    print(f"[inject] bronze_ingest exit={res.returncode}")
    # ...verify what landed in Bronze...
```

**关键**:我们**完全走真实的 Bronze ingest path**,不是直接写 parquet。这才模拟得了真上游变化。

## Detection chain — 关键剧本

跑完 inject 后,顺序观察每一层:

### Step 1: bronze_ingest

```bash
.venv/bin/python -m spark.jobs.incident_inject
```

输出:

```
[inject] wrote 200 broken PushEvent rows to /tmp/incident_2099-12-31-23.json.gz
[inject] bronze_ingest exit=0 in 12.8s
[bronze] incident partition row count: 200
[bronze] sample payload_raw key set on incident:
   ['before', 'commit_count', 'commits', 'distinct_size', 'head', 'push_id', 'ref', 'repository_id']
[bronze] notice: 'size' replaced by 'commit_count' (this is the schema break)
```

**Bronze ingest PASS**——这是设计预期。ADR-0001 把 payload 存成 raw JSON STRING,字段名变了 Bronze 不在乎。Schema-drift tolerance 在 Bronze 层是 working as designed。

### Step 2: gate_bronze

```bash
.venv/bin/python -m quality.runner --layer bronze
```

输出:

```
[PASS] bronze.events.id is unique and not_null
[PASS] bronze.events.type only in known set
[PASS] bronze.events.is_public == true
[PASS] bronze.events.created_at not_null
4 passed, 0 failed
```

**PASS 4/4**。Bronze gate 检查 envelope + 类型集合 + public + created_at,**不检查 payload 内容**。设计上就是这样——Bronze 层 payload 是黑盒。

### Step 3: silver build

```bash
cd dbt && ../.venv/bin/dbt run --select silver.events_push
```

输出:

```
OK created sql incremental model silver.events_push
Done. PASS=2 WARN=0 ERROR=0
```

**也 PASS**!这是哪里出问题了——silver build 没崩,但实际上**已经在产生坏数据**。看 SQL:

```sql
cast(get_json_object(payload_raw, '$.size') as int) as commit_size
```

新 incident 数据里 `$.size` 不存在,`get_json_object` **返回 NULL**(不抛 exception)。cast(NULL as int) = NULL。**Silver 的 200 行 `commit_size` 是 NULL**——build 不报错,内容默默 corrupt。

### Step 4: gate_silver

```bash
.venv/bin/python -m quality.runner --layer silver
```

输出(incident 加 gate 之前):

```
[PASS] silver.events_push row count == bronze.events where type='PushEvent' — silver=385,521, bronze_filtered=385,521
...
7 passed, 0 failed
```

**PASS!**——row count 是 385,521(原 385,321 + 200 incident),Bronze filter 也是 385,521。**两边都"多了 200,所以匹配"**。NULL 也是行,row count 检查不出来。

### Step 5: dbt test

```bash
cd dbt && ../.venv/bin/dbt test --select silver.events_push
```

输出:

```
9 of 9 ... unique_events_push_id ... PASS
Failure in test not_null_events_push_commit_size:
  Got 200 results, configured to fail if != 0
Done. PASS=9 WARN=0 ERROR=1
```

**这一步终于 FAIL**!`commit_size` 在 schema yml 标了 `not_null`,dbt test 跑 `select count(*) from events_push where commit_size is null` 返回 200。**dbt test 抓到了**。

但**注意时间点**:dbt test 是 Airflow DAG 的**最后一个 step**。这意味着 silver build 完成 + gate 通过 + gold build 完成 + gate 通过 + cross-mart gate 通过 + dbt test 才 fail。**如果是真生产 DAG,gold 已经被污染数据 build 过,downstream reader 已经看到了坏数字**。

## Root cause:gate-placement,不是 missing test

这就是 postmortem 的核心 insight。

**问题**:我们有 not_null 测试,但它在 pipeline **最后**跑。Silver 已经 build 完,下游已经读了。

**正确解法**:把这个 not_null check **从 dbt test 提前到 silver gate**——这样 silver build 完立刻 check,fail 时 block gold build。

修复:在 `quality/checks.py` 加一个 new check:

```python
def silver_commit_size_not_null(silver_push: DataFrame) -> CheckResult:
    """Added after incident-0001 (payload.size → payload.commit_count
    rename). Catches silent NULL coercion from get_json_object at the
    silver-gate stage so downstream Gold is never poisoned."""
    bad = silver_push.filter(F.col("commit_size").isNull()).count()
    return CheckResult(
        name="silver.events_push.commit_size not_null (incident-0001 regression gate)",
        passed=bad == 0,
        details=f"null rows={bad}",
    )
```

加进 `runner.py` 的 silver suite。**这就是 regression check**——下次同样的 root cause 在 silver gate 就 fail,不会蔓延到 gold。

同时修 model:

```sql
-- 改前
cast(get_json_object(payload_raw, '$.size') as int) as commit_size,

-- 改后
coalesce(
    cast(get_json_object(payload_raw, '$.size')         as int),
    cast(get_json_object(payload_raw, '$.commit_count') as int)
) as commit_size,
```

兼容两种字段名。`$.size` 不存在时 fallback `$.commit_count`,反之亦然。

跑一遍 full-refresh + 重新 test:

```bash
../.venv/bin/dbt run --select silver.events_push --full-refresh
../.venv/bin/dbt test --select silver.events_push
# 10/10 PASS  ← 新加的 commit_size check 也 pass
```

修复验证完成。

## Postmortem 文档 — `docs/postmortems/0001-schema-drift.md`

完整 5 Whys 段落(从原文档摘):

> 1. **Why did the bad data reach dbt test stage instead of being
>    stopped at the silver gate?**
>    Because the silver gate (`quality/checks.py`) tested row counts
>    between Bronze and Silver, but not the *contents* of the
>    payload-derived columns. NULLs count toward row counts.
>
> 2. **Why did the silver gate not test payload-derived contents?**
>    Because the Sprint 4 gate design focused on cross-layer cohesion
>    (count match, schema-set membership) on the assumption that
>    per-column null-ness was the dbt schema test's job. That
>    assumption is true only if dbt tests are wired to fail Airflow
>    tasks earlier than `dbt_test_all` runs.
>
> 3. **Why is `dbt_test_all` the last task instead of running
>    per-layer?**
>    Because in Sprint 4 the DAG was designed for a single
>    `dbt_test_all` at the end to keep the topology simple. The
>    trade-off was "if a test fails after Gold builds, we already
>    poisoned downstream readers." We accepted that trade-off
>    provisionally; this incident is the empirical reason to
>    revisit it.
>
> 4. **Why didn't anyone notice this trade-off cost was real before
>    incident-0001?**
>    Because no actual schema-break had occurred in the project's
>    sample data. The decision was theoretical until tested.
>
> 5. **Why is "no schema-break has happened yet" enough to defer
>    defenses?**
>    It isn't. ADR-0001 explicitly cites schema-drift as the design
>    horizon. The pipeline architecture (raw JSON in Bronze) is
>    drift-tolerant, but the *gate* layer wasn't proving that
>    tolerance was actionable at the right point. Theoretical
>    tolerance ≠ tested defense.

**关键句**:**"Theoretical tolerance ≠ tested defense."** —— 这是这个 drill 整个项目里最有 senior signal 的一句。背下来。

### Postmortem 的"Lessons learned"段(也建议背下)

> 1. **Schema-drift tolerance is a Bronze property, not a Silver one.**
>    Bronze swallowed the rename without complaint, exactly as
>    designed. The Silver layer needs its own drift-detection —
>    being "downstream of a drift-tolerant Bronze" is not the same
>    as being drift-tolerant itself.
>
> 2. **Gate placement matters more than gate count.** Adding more
>    tests at the end of the pipeline doesn't prevent poisoning —
>    it only detects it after the fact. The valuable gate is the
>    one between the failing layer and the next layer.
>
> 3. **A deliberate-incident drill is the only way to prove the
>    detection chain.** Five different things in this report I would
>    have got wrong from theory alone... [列举 5 个我以为对结果错的地方]
>
> 4. **One incident, one regression check.** Every postmortem
>    leaves behind at least one new check the next incident of the
>    same shape would fail on. Otherwise the postmortem is theatre.

## 验证 — 这阶段怎么知道做对了

```bash
# 跑 cleanup
.venv/bin/python -m spark.jobs.incident_inject --cleanup
# [cleanup] removing incident partition from Bronze ...
# [cleanup] done.

# 现在 silver gate 多了一个 check
.venv/bin/python -m quality.runner --layer silver
# 8 passed, 0 failed (was 7 before, now 8 with the regression check)
```

cleanup 把 incident partition 从 Bronze 删了,新 regression check 进 silver gate,以后同样的 inject 在 silver gate 就 fail——而不是流到 dbt test。

## 代码 review 笔记

`incident_inject.py` 用 `subprocess.run([".venv/bin/python", ...])` 调子进程跑 bronze_ingest。**硬编码 `.venv/bin/python`**——跟 [Ch 08](08-sprint5b-ci-and-perf.md) 的 `perf_bench.py` 同样问题。

更好做法:`from spark.jobs.bronze_ingest import main as bronze_main; bronze_main()`,直接 import。

**没改的理由**:我用 subprocess 故意保持 process-level 隔离——bronze_ingest 启动自己的 SparkSession,跟 incident_inject 的不冲突。直接 import 会在同一进程里 spin 两个 SparkSession,Spark 不太支持。

面试时这么讲:**"I used subprocess specifically because both incident_inject and bronze_ingest create their own SparkSession. Importing would cause SparkSession conflicts. The cost is hardcoding `.venv/bin/python`, which I'd parameterize for production but kept simple here."**

## You will be able to say

### 3-minute version (English) — 这一段优先背

> "Sprint 5b's second half is the deliberate incident drill. The
> goal was to test whether the pipeline's claimed schema-drift
> tolerance is real, not theoretical.
>
> I synthesized two hundred PushEvent rows where `payload.size` was
> renamed to `payload.commit_count`. Same shape otherwise, same id
> space, same created_at format. I ingested them as ingest_hour
> 2099-12-31-23 — a future date so cleanup is one DELETE statement.
>
> Then I watched every gate in the pipeline:
>
> Bronze ingest passed. By design — payload is raw JSON STRING per
> ADR-0001, so renamed fields don't break the writer.
>
> Bronze gate passed. Four checks: id unique, type in known set,
> public is true, created_at non-null. None of them inspect payload
> contents.
>
> Silver build passed. `get_json_object(payload_raw, '$.size')`
> silently returns NULL for the 200 rows. `cast(NULL as int)` is
> still NULL. The model writes 200 rows with `commit_size = NULL`.
> Build doesn't error.
>
> Silver gate passed. The row-count check: silver has 385,521 rows,
> bronze filtered to PushEvent has 385,521. They match. NULLs are
> still rows.
>
> The first detection happened at the dbt test step at the end —
> `not_null_events_push_commit_size: Got 200 results, configured to
> fail if != 0`. 
>
> The root cause isn't 'we lacked a test'. The dbt test existed and
> caught it. The problem is gate-placement — the not_null check ran
> at end-of-pipeline, by which point Gold would already have been
> built from the polluted Silver. Detection happened, but only after
> downstream poisoning was complete.
>
> The fix is two parts. First, a coalesce in events_push.sql: try
> $.size, fall back to $.commit_count. That makes the model forward
> and backward compatible. Second — and this is the senior part —
> a new regression check in `quality/checks.py` called
> `silver_commit_size_not_null`. It runs at the silver gate, between
> silver build and gold build. Now the exact same incident would
> fail the silver gate, blocking Gold build, no poisoning.
>
> The lesson I wrote in the postmortem: schema-drift tolerance is
> a Bronze property, not a Silver one. Being downstream of a
> drift-tolerant Bronze is not the same as being drift-tolerant
> yourself. And every postmortem leaves behind a regression check —
> otherwise it's theatre."

### 30-second version — when interviewer probes for one specific incident

> "I deliberately injected a schema break — `payload.size` renamed
> to `payload.commit_count` — and observed which gate caught it.
> Bronze ingest, Bronze gate, Silver build, Silver gate all passed.
> The first detection was the dbt test at end-of-pipeline, meaning
> in a real DAG, Gold would already be poisoned. The lesson:
> gate-placement, not gate count. The fix added a regression check
> at the Silver gate so the same root cause is caught before Gold
> consumes the bad data."

## 常见尖刻问题 + 准备好的答案

**Q: "为什么不直接 mock 一个 schema 改动而不是真摄入?"**

> "Mocking the change at the unit level wouldn't have surfaced the
> gate-placement insight. The point of the drill was to follow data
> through the actual pipeline — Bronze writer, Bronze gate code,
> Silver build, Silver gate, dbt test — and observe which one
> reacts. Mocks would have lost that propagation story."

**Q: "200 行的污染数据,真生产几百万行,你的 gate 都能跑吗?"**

> "The not_null check at silver gate is `df.filter(F.col(...).isNull()).count()`
> — one Spark aggregation, scans the partition. At a few million rows
> per partition, that's seconds. At billions, I'd add early-exit:
> stop the count once it exceeds threshold. The check function is
> 4 lines so easy to optimize when needed. The current implementation
> is shaped for clarity, not throughput."

**Q: "你能保证下次不同的 schema break 也能被抓?"**

> "No — and that's honest. The regression check catches this exact
> shape — null where you expect non-null. If the upstream change is
> 'string field becomes number', the cast fails and silver build
> errors at run time — different gate. If it's 'enum value adds a
> new variant', the type-in-known-set bronze check catches it.
> Different shapes need different gates. The postmortem's value
> isn't a universal defense — it's the framework for diagnosing
> the next one. Drill-then-add-regression-check is the loop."

**Q: "5 Whys 是不是太机械了?"**

> "It can be, if you stop at why #2. The discipline is to keep
> asking — most postmortems in industry stop at 'we lacked a test',
> which is why #1. Mine got to 'gate-placement decision in Sprint
> 4 was provisional, not validated' at why #5. That's the actionable
> root cause — and it told me to revisit the DAG topology, not just
> add one more check."

---

下一章 →  [10-sprint6-streaming-mvp.md](10-sprint6-streaming-mvp.md)
