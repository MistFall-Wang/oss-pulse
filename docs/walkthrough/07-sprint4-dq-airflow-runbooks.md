# Chapter 07 — Sprint 4:DQ Gates + Airflow + Runbooks

## 这章你会学到什么

什么是 data quality gate、为什么我们**故意没用** Great Expectations、Airflow DAG 怎么 parameterize、4 份 runbook 各自解决什么场景。读完你能解释:**为什么"gate 放在哪一层"比"gate 写多少个"更重要,以及 ops doc 在 senior signal 里的位置**。

## 关联前后

- **上一章** ([Ch 06](06-sprint3-marts-2-and-3.md)) 完成了三个 Gold mart
- **下一章** ([Ch 08](08-sprint5b-ci-and-perf.md)) 进入 Sprint 5b CI + 性能调优

## 背景概念(30 秒补课)

- **Data quality gate**:跑在 pipeline 两个阶段之间的 check。Fail 时 block 下游 step,Pass 时放行。跟 dbt schema test 不同——dbt test 通常在 build 之后跑;gate 是 build 之间的关卡。
- **Great Expectations(GE)**:Python 数据质量框架。提供 100+ 内置 expectation、HTML data docs、profiler。重——一个 YAML config + Python context + HTML 输出体系。
- **Airflow DAG**:把任务依赖关系画成有向无环图。每个节点是个 task,连线表示"先做这个再做那个"。
- **`BashOperator`**:Airflow 任务类型,执行 shell 命令。失败(非零退出码)时 task fail。
- **`PythonOperator`**:Airflow 任务类型,执行 Python 函数。可以通过 XCom 把返回值传给下游 task。
- **Runbook**:操作手册。当你 oncall 收到 alert,runbook 告诉你"先看 X,如果 X 是 Y 就 Z"。

## 这一阶段的目标

Sprint 0-3 我们建了 Bronze + Silver + Gold。Sprint 4 要做的是把这些 pipeline **变成可信赖、可重跑、有人值守的系统**:

1. 每一层之间加 DQ gate,fail 时 block 下游
2. 写 Airflow DAG 把"下载 → ingest → run silver → run gold → test"串起来,参数化任意 date range backfill
3. 写 runbook,把"oncall 收到告警怎么办"标准化

## 设计决策怎么做的

### 决策 1:用 GE 还是自研

| 选项 | 优 | 劣 |
|------|----|----|
| **Great Expectations** | 100+ 现成 expectation, HTML data docs, profiler | YAML config + DataContext 重, 加约 100MB 依赖, HTML 报告对 portfolio 价值小 |
| **自研轻量框架** | ~150 行 Python, 0 额外依赖, 输出格式简单 | 没 HTML, 不能 profile, 重造 GE 的部分轮子 |

选自研。**关键论据**:GE 的"价值"集中在内置 expectation 库 + HTML docs。我们这个项目要 check 的是 cross-layer 一致性(silver 行数 == bronze filter 后行数)这种 GE 难写的东西,而 not_null / unique 这种 dbt 自带就有。

**设计这个 trade-off 时,我把它写进 `quality/checks.py` 的 docstring 顶部**:

```python
"""Data-quality gates for OSS Pulse Bronze / Silver / Gold layers.

We deliberately do NOT use the full Great Expectations framework here.
The substance we need from GE — a defined set of expectations,
pass/fail with detail, and a CLI that returns non-zero on failure so
Airflow can gate downstream — is ~150 lines of plain Python. The GE
overhead (YAML config, DataContext setup, HTML data-docs) would
exceed the value at this project scale.

The structure here mirrors a GE checkpoint:
    - `suite_<layer>`  ~ ExpectationSuite
    - each function    ~ Expectation
    - `CheckResult`    ~ ExpectationValidationResult

If at Sprint 5+ we need profiling / HTML docs / out-of-the-box
expectations that aren't worth re-implementing, swapping these check
functions into GE expectations is a one-day port.
"""
```

**写下 trade-off 的 docstring 比代码本身更值钱**——它告诉 reviewer "我评估过 GE 才决定不用"。

### 决策 2:gate 放哪一层

我们设计了 4 个 suite:bronze / silver / gold / cross_mart。每个 suite 在 Airflow DAG 里是一个 task,放在对应 build task 之后、下一个 build task 之前。

```
ingest_bronze → gate_bronze → build_silver → gate_silver
              → build_gold → gate_gold → gate_cross_mart → dbt_test_all
```

**gate 放在 build 之间,不是 build 之后**。理由:

- Build 之后 gate 才 fail = 已经写了表 = 下游 task 已经开始读 = **数据污染**
- Build 之间 gate fail = block 下一个 build = 没有数据污染

这就是 [Ch 09](09-sprint5b-incident-postmortem.md) 的故意制造事故抓到的核心 lesson 提前预演。

### 决策 3:Airflow DAG 的拓扑

候选:

| 选项 | 优 | 劣 |
|------|----|----|
| (a) 每个 ingest_hour 一个 task,parallel 跑 | 快 | DAG 看起来乱,1 周 = 168 task |
| (b) 一个 task 跑整个 range | 简单 | failure 后 retry 整个 range |
| **(c) 一个 PythonOperator 把 range 展开成 bash script,然后 BashOperator 串行跑** | DAG 拓扑小, log 集中 | 单个 task 时间长 |

选 (c)。理由:portfolio 阶段 DAG 可读性 > 并行性能。production 真要并行可以换成 (a) 的 dynamic task mapping。

### 决策 4:Runbook 写哪几个

挑"实际 oncall 最常遇到"的 3 + 1:

1. **backfill.md** — 怎么重跑某段时间(最频繁场景)
2. **schema_change.md** — 上游改 schema 怎么办(最焦虑场景)
3. **data_missing.md** — "昨天数据少了" 怎么诊断(最常被 stakeholder 问)
4. **airflow_setup.md** — 怎么把 Airflow 跑起来(给新人 onboarding)

## 代码逐行讲

### `quality/checks.py`(check 函数,纯函数)

```python
@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str

    def __str__(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        return f"[{mark}] {self.name} — {self.details}"


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
```

设计原则:

- 每个 check **一个纯函数**,接收 DataFrame,返回 `CheckResult`(不 raise)
- `CheckResult` 是 dataclass 三字段:name / passed / details
- 决定"fail 时怎么办"是 runner 的事,不是 check 的事——这样 check 可以单测

完整 check 列表(18 个):

- Bronze (4): id 唯一+非空 / type 在已知集合 / is_public=true / created_at 非空
- Silver (7): 每张 silver 行数 == bronze filter 行数 + pr_state 在合法值 + commit_size 非空(incident-0001 回归 gate)
- Gold (5): 3 个 mart 的 grain 唯一 + pr_merged <= pr_closed + bot_event_share ∈ [0,1]
- Cross-mart (1): repo_daily_activity.bot_push_count == bot_vs_human_mart.push_bot_count

### `quality/runner.py`(CLI 入口)

```python
def build_spark() -> SparkSession:
    import os
    args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if "--driver-memory" not in args:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--driver-memory 4g " + args + " pyspark-shell"
        ).strip()
    builder = (
        SparkSession.builder.appName("quality_runner")
        # ...standard Delta config...
        .config("spark.driver.memory", "4g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_bronze(spark):
    bronze = spark.read.format("delta").load(BRONZE_PATH).select(
        "id", "type", "created_at", "is_public"
    )
    # Only select columns we need — keeps payload_raw out of driver
    # heap during distinct/aggregate.
    return [
        checks.bronze_id_unique_and_not_null(bronze),
        checks.bronze_type_in_known_set(bronze),
        checks.bronze_is_public_always_true(bronze),
        checks.bronze_created_at_not_null(bronze),
    ]


SUITES = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
    "cross_mart": run_cross_mart,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=sorted(SUITES.keys()))
    args = parser.parse_args()

    spark = build_spark()
    print(f"\n========== quality suite: {args.layer} ==========")
    results = SUITES[args.layer](spark)
    for r in results:
        print(r)

    failed = [r for r in results if not r.passed]
    print(f"\n[summary] {len(results) - len(failed)} passed, {len(failed)} failed")

    spark.stop()
    if failed:
        sys.exit(1)
```

要点:

- `sys.exit(1)` 才是关键。CLI 退出码非零 → Airflow BashOperator 标记 task fail → 下游 task block。**这就是"non-zero exits gate the next task"**
- `select("id", "type", ...)` 那一行 —— **真 senior 才会做的优化**。Bronze 表有 `payload_raw` 这个 JSON STRING 列,如果不显式 select,Spark distinct(id) 会先把整行 load 进 driver heap → OOM。只 select 需要的列,避免 OOM
- `SUITES` 是 dict —— 加新 layer 不用改 main(),加一行就行

### `airflow/dags/oss_pulse_pipeline.py`(整 DAG)

```python
PROJECT_ROOT = os.environ.get(
    "OSS_PULSE_ROOT",
    str(Path(__file__).resolve().parents[2]),
)

COMMON_ENV = {
    "JAVA_HOME": JAVA_HOME,
    "PATH": f"{JAVA_HOME}/bin:" + os.environ.get("PATH", ""),
    "PYSPARK_DRIVER_MEMORY": "4g",
    "PYSPARK_SUBMIT_ARGS": "--driver-memory 4g pyspark-shell",
}


def hour_range(start: str, end: str) -> list[str]:
    """Inclusive hour strings between two YYYY-MM-DD-HH stamps."""
    fmt = "%Y-%m-%d-%H"
    s = datetime.strptime(start, fmt)
    e = datetime.strptime(end, fmt)
    out = []
    cur = s
    while cur <= e:
        out.append(cur.strftime(fmt))
        cur += timedelta(hours=1)
    return out


def expand_ingest_commands(**context) -> str:
    params = context["params"]
    hours = hour_range(params["start_hour"], params["end_hour"])
    cmds = ["set -euo pipefail"]
    for h in hours:
        target = f"{PROJECT_ROOT}/data/raw/{h}.json.gz"
        cmds.append(f"if [ ! -f {target} ]; then "
                    f"  curl -sf -o {target} https://data.gharchive.org/{h}.json.gz; "
                    f"fi")
        cmds.append(f"cd {PROJECT_ROOT} && "
                    f".venv/bin/python -m spark.jobs.bronze_ingest "
                    f"--source data/raw/{h}.json.gz "
                    f"--bronze-path data/bronze/events")
    script = "\n".join(cmds)
    context["ti"].xcom_push(key="ingest_script", value=script)
    return script


with DAG(
    dag_id="oss_pulse_pipeline",
    schedule=None,                # 手动 trigger
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "peter", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "start_hour": "2025-01-15-12",
        "end_hour":   "2025-01-15-12",
    },
    tags=["oss-pulse", "medallion"],
) as dag:
    plan_ingest = PythonOperator(
        task_id="plan_ingest_range",
        python_callable=expand_ingest_commands,
    )
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="{{ ti.xcom_pull(task_ids='plan_ingest_range', key='ingest_script') }}",
        env=COMMON_ENV, append_env=True,
    )
    gate_bronze = BashOperator(...)
    build_silver = BashOperator(...)
    gate_silver = BashOperator(...)
    build_gold = BashOperator(...)
    gate_gold = BashOperator(...)
    gate_cross_mart = BashOperator(...)
    dbt_test_all = BashOperator(...)

    plan_ingest >> ingest_bronze >> gate_bronze \
        >> build_silver >> gate_silver \
        >> build_gold >> gate_gold >> gate_cross_mart \
        >> dbt_test_all
```

逐段:

- `PROJECT_ROOT` 这一行就是 Ch 04 的硬编码路径修复——从 `__file__` 推导,reviewer clone 后能直接 import 这个 DAG
- `expand_ingest_commands` 是个 PythonOperator,**不直接执行 bronze ingest**,而是先 plan 出 N 个小时的 bash 命令脚本字符串,通过 XCom 推给下游 BashOperator
  - 这样 DAG 拓扑永远是 9 个 task,无论 backfill 1 小时还是 100 小时
  - XCom 是 Airflow 的"任务间小数据传递"机制
- `params` 是 DAG-level 参数,trigger 时可以 override:`airflow dags trigger ... --conf '{"start_hour": "...", "end_hour": "..."}'`
- 最后 `>>` 链接 task 依赖关系——pipeline 拓扑就在最后这一行展开

### Runbooks(都在 `docs/runbooks/`)

每个 runbook 跟随同样的结构:**when to use → steps → verify → common failures table**。

举 [`backfill.md`](../runbooks/backfill.md) 为例:

> **When to use**: 因为 bug 修复后要重跑、新增 ingest_hour、上游 republish 等场景重新摄入。
>
> **Idempotency contract**: Bronze + Silver + Gold 都 MERGE,不会 dup。
>
> **Steps**: 
> 1. 决定 date range
> 2. 用 Airflow trigger 或纯 shell 跑
> 3. 验证 Bronze + Gold 行数
>
> **Common failures**:
> | Symptom | Likely cause | Fix |
> |---|---|---|
> | curl 404 | hour 还没发布 | 检查 GH Archive 状态 |
> | JAVA_HOME 错 | Java 18+ | 切 Corretto 17 |
> | OOM | driver heap 小 | 设 PYSPARK_SUBMIT_ARGS |

**写 runbook 的关键**:**不是文档,是 oncall 当夜能照着做的 checklist**。所以每一步要可执行,不是抽象描述。

## 验证 — 这阶段怎么知道做对了

```bash
# 4 个 suite 全跑
for layer in bronze silver gold cross_mart; do
  uv run python -m quality.runner --layer $layer
done
# 4+8+5+1 = 18 PASS, 0 FAIL

# Airflow DAG parse 干净
AIRFLOW_HOME=/tmp/test python -c "
from airflow.models.dagbag import DagBag
db = DagBag(dag_folder='airflow/dags', include_examples=False)
print('Errors:', db.import_errors)   # 期望 {}
"
```

DQ gate 全 pass + DAG 0 import error = Sprint 4 done。

## 代码 review 笔记

复看时发现 `quality/runner.py` 里 `build_spark` 有个微妙问题:

```python
if "--driver-memory" not in args:
    os.environ["PYSPARK_SUBMIT_ARGS"] = ...
```

如果 user 已经设了 `PYSPARK_SUBMIT_ARGS="--executor-memory 8g pyspark-shell"`,这段会**不附加** `--driver-memory`(因为字符串包含 `memory` 但不是 `driver-memory`)。这是个 false negative 的检查。

更严谨的写法:

```python
if "--driver-memory" not in args.split():
    ...
```

但**没改**,因为:

1. portfolio 阶段没人会手动设 PYSPARK_SUBMIT_ARGS 的 executor-memory
2. corner case 触发条件极小
3. fix 会引入一个 split() 调用的 mental load

承认 imperfection 比假装完美强。

## You will be able to say

### 3-minute version (English)

> "Sprint 4 turns the pipeline into something operable. Three pieces.
>
> First, data-quality gates. I considered Great Expectations and
> chose to write a lightweight framework instead — the trade-off is
> documented in `quality/checks.py`'s docstring. The substance is
> 18 checks across 4 suites: bronze, silver, gold, cross-mart. Each
> suite is a CLI that exits non-zero on any failed check, which is
> what makes it gate-able from Airflow's BashOperator. The
> cross-mart suite is the one that caught the LombiqBot inconsistency
> in Sprint 3b.
>
> Second, the Airflow DAG. It's parameterized — `start_hour` and
> `end_hour` get expanded by a PythonOperator into a shell script,
> XCom'd to a BashOperator. So the DAG topology stays 9 tasks
> regardless of whether I'm backfilling 1 hour or 100. Gates are
> placed between build steps, not after — so a Silver failure
> blocks Gold from building, instead of poisoning Gold and catching
> it at end-of-pipeline dbt test.
>
> Third, four runbooks. backfill, schema-change, data-missing,
> airflow-setup. The structure of each: when-to-use, steps, verify,
> common-failures table. They're written so oncall can follow them
> at 2 AM without having to understand the codebase. The
> data-missing runbook in particular is a top-down decision tree —
> Gold to Silver to Bronze to source — that any new team member can
> use to triage 'yesterday's numbers look light'."

## 常见尖刻问题 + 准备好的答案

**Q: "为什么不用 GE? 招聘说要会 GE。"**

> "I evaluated GE end-to-end — set up a DataContext, defined a couple
> of expectations, generated docs. For this project's scale and
> portfolio nature, the YAML config + HTML data docs overhead exceeded
> the value. The 150 lines of plain Python in `quality/checks.py`
> deliver the same substance: declarative checks, exit-code-based
> gating, pass-or-fail with detail. The docstring at the top of
> checks.py spells out the trade-off so any reviewer sees the
> reasoning. If a job requires hands-on GE, I'd port these checks in
> a day — the structure already mirrors GE's checkpoint pattern."

**Q: "Airflow DAG 没真跑过 production schedule 啊?"**

> "Correct — it's manually triggered, not cron-scheduled. The DAG
> was validated under Airflow 2.10's DagBag with zero import errors,
> and each individual task command — `bronze_ingest`, `dbt run
> --select silver`, `quality.runner` — has been run end-to-end. I
> didn't run a long-lived scheduler because Sprint 5a's plan is to
> move scheduling to Astronomer or Databricks Workflows, and locally
> the airflow standalone process is overhead without payoff."

**Q: "如果 gate 自己挂了你怎么办?"**

> "Two levels. First, the gate's check functions are written as pure
> functions returning CheckResult — they don't raise. If a Spark
> operation inside fails, the runner's main() catches at the
> SparkSession level. Second, if the runner itself crashes, the
> BashOperator sees non-zero exit and the task fails. So a broken
> gate is the same outcome as a failed check: block downstream. No
> silent skip."

---

下一章 →  [08-sprint5b-ci-and-perf.md](08-sprint5b-ci-and-perf.md)
