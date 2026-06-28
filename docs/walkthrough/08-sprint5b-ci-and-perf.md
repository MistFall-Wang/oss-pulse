# Chapter 08 — Sprint 5b 上半:CI + 性能调优诚实负结果

## 这章你会学到什么

GitHub Actions CI 怎么搭、为什么我们的 perf 实验**没成功但反而成了 senior 信号**。读完你能解释:**OPTIMIZE+ZORDER 在小规模 Bronze 上为什么没起作用、OPTIMIZE 临时翻 2× 存储是怎么回事、为什么 ADR-0009 从"待办"变成"必做"**。

## 关联前后

- **上一章** ([Ch 07](07-sprint4-dq-airflow-runbooks.md)) 给整个 pipeline 加 DQ gate + Airflow + runbook
- **下一章** ([Ch 09](09-sprint5b-incident-postmortem.md)) Sprint 5b 下半:故意造事故 + postmortem

## 背景概念(30 秒补课)

- **GitHub Actions**:GitHub 自带的 CI/CD 系统。在 `.github/workflows/` 写 YAML 描述什么时候跑、跑什么。
- **`OPTIMIZE`(Delta 命令)**:把一个 Delta 表的小文件合并成大文件,减少 metadata 开销。不删旧文件(等 VACUUM)。
- **`ZORDER BY <col>`**:`OPTIMIZE` 的 modifier。在 compact 时按某列把行排到一起,后续按那列 filter 时可以 skip 整个文件(data skipping)。
- **`VACUUM`**:删除 Delta 表里"过保的"旧文件版本。生产中通常 retain 7 天,dev 可以 retain 0 小时立刻删。
- **`spark.read.format("delta").load(path).inputFiles()`**:返回这个 DataFrame 实际会读的 parquet 文件列表。数它的长度可以判断 data skipping 起没起作用。

## 这一阶段的目标

Sprint 5b 上半两件事:

1. 写 GitHub Actions CI,每次 push / PR 自动跑 ruff + pytest + dbt parse + compile
2. 跑一次"我用 ZORDER 能让 Silver build 快吗"的性能实验,**记录 before/after 5 维度数据**,**包括负结果**

## 设计决策怎么做的

### 决策 1:CI 跑哪几个 job

候选:

- (a) 只跑 ruff
- (b) ruff + pytest
- (c) ruff + pytest + dbt parse
- (d) **ruff + pytest + dbt parse + dbt compile**(实际选)
- (e) (d) + dbt run + dbt test(需要 warehouse)
- (f) 加 terraform plan + airflow validate

选 (d)。理由:

- (a)/(b) 太弱,SQL 错没人 catch
- (c)/(d) 是 sweet spot——验证项目结构 + SQL 语法 + ref 解析,**不需要 warehouse**
- (e) 需要在 CI 里跑 Spark,build 时间从 1 分钟涨到 5+ 分钟,得不偿失
- (f) 应该加但没加,**这是 8/10 评分里"CI 偏静态"的扣分点**

### 决策 2:ZORDER 性能实验的假设

Bronze 按 `ingest_hour` 分区,每个 Silver model 都 filter `where type = 'PushEvent'`(或别的)。filter 列 `type` 跟分区列 `ingest_hour` 独立——同一个 hour 的文件里 15 种 type 混在一起。

**假设**:`OPTIMIZE ... ZORDER BY (type)` 把同一 type 的行聚到同一文件里,这样 Silver build 的 filter 可以 skip 大部分文件。

期望 metrics 改善:

- Bronze 读取的 bytes 大幅减少
- Silver build wall-clock 减少
- inputFiles() 数减少(直接证明 data skipping)

代价:OPTIMIZE 自身有 wall-clock 开销 + 临时 storage 翻倍(写新文件不删旧)

## 代码逐行讲

### `.github/workflows/ci.yml`

```yaml
name: CI

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  static:
    name: Lint + format
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install uv
        uses: astral-sh/setup-uv@v3
        with:
          version: "0.8.12"
      - run: uv python install 3.11
      - run: uv sync --frozen
      - run: uv run ruff check .
      - run: uv run ruff format --check .

  unit-tests:
    name: pytest (Spark unit tests on JDK 17)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv python install 3.11
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'
      - run: uv sync --frozen
      - env:
          PYSPARK_SUBMIT_ARGS: "--driver-memory 2g pyspark-shell"
        run: uv run pytest spark/tests/ -v

  dbt-static:
    name: dbt parse + compile
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv python install 3.11
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'
      - run: uv sync --frozen
      - working-directory: dbt
        run: uv run --project .. dbt deps
      - run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml <<'EOF'
          oss_pulse_dbt:
            target: ci
            outputs:
              ci:
                type: spark
                method: session
                schema: silver
                host: NA
          EOF
      - working-directory: dbt
        run: uv run --project .. dbt parse
      - working-directory: dbt
        run: uv run --project .. dbt compile
```

逐 job:

**`static`**:ruff check(lint)+ format check。失败说明有人没跑 `ruff format` 就 commit。

**`unit-tests`**:跑 `pytest spark/tests/` 那 3 个 case(extract_ingest_hour 测试)。JDK 17 必须装(Spark 3.5 + Java 18 崩),`PYSPARK_SUBMIT_ARGS` 给 driver 加 heap(避免 GitHub runner 2GB RAM OOM)。

**`dbt-static`**:重点。

- 写一个 CI-only `profiles.yml`,target name = `'ci'`
- `dbt deps` 装 dbt-utils
- `dbt parse` 检查项目结构和 Jinja
- `dbt compile` 检查每个 model 的 SQL 编译——但**触发 on-run-start hook**,会跑 `register_external_sources` macro

这个 macro 在 [Ch 03](03-sprint1-first-silver.md) 写了 `{% if target.name == 'ci' %}` 分支,所以 CI 跑时 skip 掉 CREATE TABLE—— 因为 CI 的 Spark 没有 Delta jars 装到 classpath。**这个 if 分支是 Ch 03 之后实际被 CI 推着加上的**(最初我也没料到)。

### 性能实验的脚本 `spark/jobs/perf_bench.py`

```python
SILVER_MODELS = [
    ("events_push", "PushEvent"),
    ("events_pull_request", "PullRequestEvent"),
    ("events_issue_comment", "IssueCommentEvent"),
    ("events_issues", "IssuesEvent"),
    ("events_watch", "WatchEvent"),
    ("events_fork", "ForkEvent"),
]


def bronze_filter_metrics(spark, event_type) -> dict:
    df = (spark.read.format("delta").load(BRONZE_PATH)
                                    .filter(F.col("type") == event_type))
    t0 = time.perf_counter()
    rows = df.count()
    wall = time.perf_counter() - t0

    files_read = len(df.inputFiles())   # !!! 关键:实际读多少文件
    bronze_total = dir_bytes(BRONZE_PATH)
    return {
        "wall_seconds": round(wall, 2),
        "rows": rows,
        "bronze_files_after_prune": files_read,
        "bronze_total_bytes": bronze_total,
    }


def measure_round(spark, label) -> dict:
    silver_wall = run_dbt_silver_build(full_refresh=True)
    per_type = {}
    for model_name, event_type in SILVER_MODELS:
        per_type[model_name] = {
            **bronze_filter_metrics(spark, event_type),
            **silver_state(model_name),
        }
    return { "label": label, "silver_full_refresh_seconds": silver_wall,
             "per_silver_model": per_type, ... }


def run_optimize_zorder(spark) -> dict:
    t0 = time.perf_counter()
    DeltaTable.forPath(spark, BRONZE_PATH).optimize().executeZOrderBy("type")
    return { "wall_seconds": round(time.perf_counter() - t0, 2),
             "bronze_files_before": ..., "bronze_bytes_before": ...,
             "bronze_files_after": ..., "bronze_bytes_after": ... }


def main():
    spark = build_spark("perf_bench")
    before = measure_round(spark, "BEFORE")
    opt = run_optimize_zorder(spark)
    after = measure_round(spark, "AFTER")
    # ... write JSON report ...
```

要点:

- `measure_round` 跑 dbt silver build + 对每张 silver 测 4 个 metric
- 实验设计:跑两轮(before / after)+ 中间一次 OPTIMIZE
- **`inputFiles()` 是黄金 metric**——告诉你 data skipping 真起没起作用,不被 cache 或 JIT 干扰

### 实测结果

跑出来的真实数字(摘):

```
[BEFORE] silver build wall clock: 37.36s
  events_push            filter→2.73s, files_read=4, out_files=15, out_bytes=95,527,408
  events_pull_request    filter→0.21s, files_read=4, ...
  events_issue_comment   filter→0.18s, files_read=4, ...
  events_issues          filter→0.16s, files_read=4, ...
  events_watch           filter→0.19s, files_read=4, ...
  events_fork            filter→0.14s, files_read=4, ...

[OPTIMIZE] done in 6.9s
[OPTIMIZE] bronze files: 4 → 8; bytes: 465,703,375 → 931,403,461   ← !!! 2× storage

[AFTER] silver build wall clock: 38.34s
  events_push            filter→0.89s, files_read=4, ...    ← faster but same files!
  events_pull_request    filter→0.17s, files_read=4, ...
  (其余基本不变)
```

**核心发现 1**:`events_push` filter 从 2.73s → 0.89s,看起来快了 3 倍。

**核心发现 2**:`files_read=4` 在 before 和 after **完全一样**。data skipping **没起作用**。

**核心发现 3**:Silver full build 37.36s → 38.34s。没改善,甚至略慢。

**核心发现 4**:OPTIMIZE 后 Bronze 文件数 4→8,字节数 465MB→931MB。**临时翻 2×**。

### 解读这些数字 —— senior signal 的真正所在

如果我看到"`events_push` 从 2.73s → 0.89s"就高兴地写"ZORDER 优化成功 3 倍",这就是 junior。

**真相**:`inputFiles()=4` 双 round 一样,**没 skip**。那 2.73 → 0.89 的速度提升是 **JVM JIT 已经热身 + OS file cache hot** 造成的——after round 是第二次跑,JVM 已经认识这段代码,OS 已经把文件缓存在 RAM。

**为什么 ZORDER 在我们规模下无效**:Bronze 只有 4 partition(4 个 ingest_hour),每个 partition 1 个文件,共 4 个文件。ZORDER 在文件内重排行,但**不切分文件**。data skipping 的最小粒度就是"跳过整个文件",4 个文件 = 最小 skip 25%。ZORDER 没空间发挥。

**OPTIMIZE 临时 2× storage**:OPTIMIZE 写新 compact 文件,旧文件不删,**等 VACUUM**。我们跑 VACUUM 之后:

```
[before VACUUM] bytes=931 MB files=8
[vacuum] running VACUUM ... RETAIN 0 HOURS on Bronze ...
[after VACUUM] bytes=465 MB files=4
```

回到 465 MB。但**在生产环境,Delta 默认 retain 7 天**,意味着 OPTIMIZE 后 7 天内 Bronze 多占 1× storage。**这是 ADR-0009 (OPTIMIZE/VACUUM cadence) 必须存在的 empirical 理由**。

## 这个负结果怎么写 — `docs/performance/sprint5b_tuning.md`

报告的结构:

1. **Hypothesis**(假设)
2. **What was measured**(5 维度)
3. **Results — the headline**(数字表)
4. **Why it didn't help (root cause)**(用 inputFiles() 解释)
5. **Storage cost of the experiment**(2× table)
6. **What would actually help at this scale**(下一步:dbt cold-start 是更大瓶颈)
7. **What an interviewer should hear**(自我审视段)

关键段落 — 我直接抄一段进 walkthrough,你面试可以背:

> "I had a hypothesis that ZORDER would speed up per-type filters.
> I measured before and after across 5 dimensions per model. The
> headline wall-clock looked faster, but `inputFiles()` showed zero
> pruning — the speedup was warmup, not ZORDER. I confirmed the
> root cause (4 files = 4-partition Bronze, smaller than ZORDER's
> prune unit) and refused to call the experiment a win. The storage
> trade-off (2x footprint until VACUUM) was the data point that
> promoted ADR-0009 from an aspirational Sprint 9 entry to a real
> production commitment."

**为什么这段是 senior signal**:

- 有假设、有实验、有数据
- **拒绝把假性加速当成结果**
- 解释了根本原因(粒度 / 文件数)
- 把负结果转化为另一个 ADR 的必要性论据

junior 写"我做了 OPTIMIZE,events_push 提速 3 倍",senior 写"我做了 OPTIMIZE,实测没生效,这告诉我...同时让 ADR-0009 必须做"。

## 验证 — 这阶段怎么知道做对了

CI:

```bash
# 本地预跑 CI 三个 job 内容,确保跟 GitHub Actions 跑出来一致
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/pytest spark/tests/ -v          # 3 PASS
cd dbt && ../.venv/bin/dbt deps && ../.venv/bin/dbt parse && ../.venv/bin/dbt compile
# 所有 OK
```

Push 触发 CI,看 GitHub Actions 三个 job 全绿。

Perf 报告写完 = Sprint 5b 上半 done。

## 代码 review 笔记

`perf_bench.py` 用 `subprocess.run(["../.venv/bin/dbt", ...])` 调 dbt——硬编码 `.venv/bin/dbt` 路径,反 senior。

更对的做法是 `subprocess.run(["uv", "run", "--project", "..", "dbt", ...])`,跨 venv 都能跑。

**没改的理由**:这是探索 / benchmark 脚本,不是 production code,跑一次出报告就完事。如果改成 production 自动化,会去掉这个硬编码。

## You will be able to say

### 3-minute version (English)

> "Sprint 5b's first half: CI plus a performance tuning experiment
> with an honest negative result.
>
> The CI is GitHub Actions, three jobs: lint with ruff, pytest on
> JDK 17, and dbt parse + compile. The dbt-compile step taught me
> something — it triggers the on-run-start hook, which tried to do
> CREATE TABLE USING delta and failed because CI's Spark doesn't
> have the Delta jars on classpath. Fix was an `if target.name ==
> 'ci'` guard in the macro. That's the kind of bug CI catches that
> local dev can't.
>
> The performance experiment: my hypothesis was that
> OPTIMIZE+ZORDER BY type on Bronze would prune per-type filter
> reads. I measured before and after across five dimensions per
> Silver model: bronze filter wall-clock, files read after prune,
> output rows, output file count, output bytes. Plus the full
> Silver-build wall-clock as the headline.
>
> Result: ZORDER didn't help. `events_push` filter went from 2.73
> seconds to 0.89, looks like a 3x speedup, but `inputFiles()` was
> the same 4 files before and after — same as the partition count,
> no pruning. The wall-clock improvement was JIT warmup and OS file
> cache, not ZORDER. The full Silver build was 37 vs 38 seconds —
> no net change. Storage side: OPTIMIZE temporarily doubled Bronze
> from 465 MB and 4 files to 931 MB and 8 files, until VACUUM
> brought it back.
>
> The reason ZORDER didn't help: my Bronze has only 4 files. ZORDER
> reorders rows within a file but can't split files. Data skipping's
> minimum unit is one file — at 4 files, the minimum skip is 25%
> of the table. ZORDER had no room to work. At 100x the scale,
> worth re-running. The 2x storage spike is the empirical reason
> ADR-0009 — VACUUM cadence — is a production must, not an
> aspirational future ADR. The whole experiment is in
> `docs/performance/sprint5b_tuning.md` with raw JSON output for
> reproducibility."

## 常见尖刻问题 + 准备好的答案

**Q: "CI 没跑 dbt build / dbt test 是不是太弱?"**

> "Yes — by design. dbt build requires a warehouse with Delta jars,
> which means either provisioning Delta in CI's Spark (slow + flaky)
> or pointing CI at a real warehouse (cost + secrets). Sprint 5a's
> roadmap adds a `dbt-prod.yml` workflow that triggers on merge to
> main and runs against Databricks SQL. The PR-time CI stays static
> for speed. If hiring criteria require real dbt build in CI, the
> infrastructure is one ADR away — the trade-off is in Sprint 5a's
> walkthrough."

**Q: "你 perf 实验只测了一个 hypothesis?"**

> "Yes — focused. I measured one specific question — does ZORDER
> prune per-type Silver reads — with five dimensions of data per
> table. The negative result was definitive, with `inputFiles()`
> proving the mechanism. A second experiment, ZORDER by `repo_id`
> for the Gold side, is the natural follow-up — but Bronze at 4
> partitions doesn't let it shine, so I'd defer to a 7-day backfill
> first. That follow-up is recorded as the report's last paragraph."

**Q: "如果你的 perf 实验真的没起作用,为什么留着 OPTIMIZE 命令?"**

> "I didn't keep it in the pipeline. The OPTIMIZE was a one-off
> experiment via `perf_bench.py`, not a scheduled step. The pipeline
> writes Bronze + Silver + Gold without OPTIMIZE. ADR-0009 will
> codify a cadence — compact daily, vacuum weekly with 168h
> retention — once Bronze grows past the threshold where pruning
> can actually fire."

---

下一章 →  [09-sprint5b-incident-postmortem.md](09-sprint5b-incident-postmortem.md)
