# Chapter 03 — Sprint 1 step 4:第一个 Silver 模型

## 这章你会学到什么

dbt 是什么、为什么用 dbt 而不是直接写 Spark SQL、第一个 Silver 模型 `events_push` 怎么从 Bronze 解析出来。读完你能解释:**为什么 dbt 的 `ref()` 和 `source()` 比手写 Spark `spark.read.table()` 强、为什么 `on_schema_change='fail'` 是一个看似偏执但 senior 必备的配置**。

## 关联前后

- **上一章** ([Ch 02](02-sprint1-bronze-ingestion.md)) 把 Bronze 表搭好了
- **下一章** ([Ch 04](04-sprint2-first-gold-mart.md)) 在 Silver 之上建第一个 Gold mart

## 背景概念(30 秒补课)

- **dbt(data build tool)**:你写 `.sql` 文件描述每个表怎么算,dbt 帮你处理 ① 自动建表/插数据 ② 表与表之间的依赖关系 ③ 跑 schema test 自动验证。简而言之:用 SQL 写"表的生命周期"。
- **dbt-spark**:dbt 的 Spark 适配器。dbt 自己跟 warehouse 无关,通过 adapter 跟具体引擎对接。我们这里 adapter 是 dbt-spark + session mode(把 Spark 直接 embed 进 dbt 进程)。
- **dbt model**:`.sql` 文件,顶部是 `{{ config(...) }}` block,接着是一个 `SELECT`。dbt 用这个 SELECT 创建表或插入数据。
- **`{{ source('bronze', 'events') }}`**:dbt 的 source() 函数,在 `sources.yml` 里定义,引用上游(non-dbt-managed)表。返回一个 SQL 表引用。
- **`{{ ref('events_push') }}`**:dbt 的 ref() 函数,引用另一个 dbt model。这是 dbt 的精髓——你不写表名,你写 ref;dbt 自动算出依赖关系图。
- **`{{ config(materialized=...) }}`**:这个模型用什么方式落到 warehouse:`view`(虚拟视图,每次查询重算)/ `table`(固化成表,每次 dbt run 重建)/ `incremental`(增量,每次 dbt run 只处理新数据 + merge)。
- **dbt macro**:类似 Python 函数,你写一段可重用的 Jinja。`{{ delta_source('bronze', 'events') }}` 是我们自己写的 macro。
- **`on-run-start` hook**:dbt 在每次跑前先执行一段 SQL/macro,用来准备 environment(比如 register external table)。

## 这一阶段的目标

Bronze 表 `bronze.events` 有 15 种 event type 混在一起,每行的 `payload_raw` 是 JSON STRING。Sprint 1 step 4 要做的是:

1. 把 dbt 项目搭起来
2. 写第一个 Silver model `events_push`,从 Bronze 里 filter type='PushEvent' 然后解析 `payload_raw` 抽出 PushEvent 特有的字段(push_id, commit_size, ref, head_sha 等)
3. 让这个 model 是 incremental 的——每次只处理新的 ingest_hour
4. 给它写 schema test:id unique + not_null + 业务规则

## 设计决策怎么做的

### 决策 1:为什么用 dbt 而不是 Spark SQL / Python

| 方式 | 优 | 劣 |
|------|----|----|
| Spark SQL 直接写 `.sql` 文件,自己写 runner | 直接 | 依赖关系自己管;测试要自己写 framework;增量逻辑自己写 |
| Python + Spark DataFrame API | 类型安全 | 一个 SELECT 写成 50 行 |
| **dbt** | model 之间依赖自动算;schema test 是 declarative;incremental 是 config 一行;社区 macros 库丰富 | 多一层抽象;调试 SQL 比直接写麻烦 |

选 dbt。理由是 Senior DE 在加拿大求职市场上几乎所有 JD 都要求 dbt——这是个市场信号,不是技术信号。

### 决策 2:用 dbt-spark 还是 dbt-databricks

`dbt-spark` 跟本地 Spark(包括 session mode)兼容,**离线开发不需要任何云**。`dbt-databricks` 是 Databricks 推荐的 adapter,但需要连一个真的 Databricks SQL warehouse。

我们 Sprint 1-4 全程用 dbt-spark + session mode 离线开发,Sprint 5a 才计划切换。**ADR-0005** 里写了切换计划和需要 audit 的 macro。

### 决策 3:`on_schema_change` 选什么值

dbt 的 incremental model 有四种 schema change 行为:

- `ignore`:默认。新加的字段被静默丢弃 ☠
- `append_new_columns`:新加字段被自动添加到 target table
- `sync_all_columns`:target schema 跟 source 完全 sync
- `fail`:schema 变化时 build 直接失败

**我们选 `fail`**。理由:Bronze 的 schema 我们已经 ADR-0001 严格控制,Silver 不应该因为"Spark 加了个新列"就静默改 schema。**显式比隐式好**。

## 代码逐行讲

### `dbt/dbt_project.yml`(项目根 config)

```yaml
name: 'oss_pulse_dbt'
profile: 'oss_pulse_dbt'

model-paths: ["models"]
macro-paths: ["macros"]
seed-paths: ["seeds"]
# ...

on-run-start:
  - "{{ register_external_sources() }}"

models:
  oss_pulse_dbt:
    +file_format: delta
    silver:
      +materialized: table
      +schema: silver
    gold:
      +schema: gold

vars:
  bronze_events_path: "{{ env_var('OSS_PULSE_BRONZE_PATH', '../data/bronze/events') }}"
```

逐项:

- `on-run-start` 在每次 `dbt run` 前先跑 `register_external_sources()` 这个 macro——它告诉 Spark"`bronze.events` 这个表的物理位置在哪个目录",这样 ref() / source() 才能解析
- `+file_format: delta`:所有 model 默认用 Delta 格式写
- `silver: +materialized: table`:Silver 默认全表重建。但其实每个 model 在 `.sql` 顶部又用 `config(materialized='incremental')` 覆盖了——这里写 `table` 是 fallback
- `+schema: silver`:Silver model 的 schema 名(数据库名)
- `vars`:**这里就是 Ch 02 fix 的硬编码路径**。现在用 `env_var('OSS_PULSE_BRONZE_PATH', '../data/bronze/events')`——优先看环境变量,没有就用 repo-relative 路径。reviewer clone 后直接能跑

### `dbt/macros/register_external_sources.sql`

```jinja
{% macro register_external_sources() %}
    {% if execute %}
        {% if target.name == 'ci' %}
            {{ log("[register_external_sources] skipped on CI target", info=True) }}
        {% else %}
            {% set sources_to_register = [
                ('bronze', 'events', var('bronze_events_path', none)),
            ] %}
            {% for source_name, table_name, path in sources_to_register %}
                {% if path %}
                    {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ source_name) %}
                    {% do run_query(
                        "CREATE TABLE IF NOT EXISTS " ~ source_name ~ "." ~ table_name ~
                        " USING DELTA LOCATION '" ~ path ~ "'"
                    ) %}
                    {{ log("[register_external_sources] registered ...", info=True) }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}
```

读这段:

- `{% if execute %}`:dbt parse 阶段(不执行 SQL)skip 掉,只在真 run 时执行
- `{% if target.name == 'ci' %}`:这就是 [Ch 08](08-sprint5b-ci-and-perf.md) 修过的 CI fix。CI target 没有 Delta jars,CREATE TABLE USING delta 会崩,所以 skip
- `CREATE TABLE IF NOT EXISTS bronze.events USING DELTA LOCATION '<path>'`:把物理路径注册成 Spark 元存的"外部表"。这样 dbt model 里写 `source('bronze', 'events')` 就能解析成这张表

为什么用 macro 而不是直接在 dbt_project.yml 写 hook?**因为 macro 可以测、可以扩展(下个 Sprint 加 source 时只改 list)、可以用 Jinja 控制 if/else**。

### `dbt/macros/delta_source.sql`

```jinja
{% macro delta_source(source_name, table_name) %}
    {{ source(source_name, table_name) }}
{% endmacro %}
```

**这个看着没意义,但有意义**。它就是 dbt 内置 `source()` 的 wrapper。为什么写它?

理由 1:**意图明确**。读 model 时看到 `delta_source(...)` 知道这是来自 Delta 外部表;看到 `source(...)` 不知道是 Delta 还是 view 还是别的。

理由 2:**未来加 cross-cutting 行为不用改 model**。比如 future 想给所有 Bronze 读自动 cache,在 `delta_source` 里加一行就够;如果直接调 `source`,每个 model 都要改。

这种"先用 wrapper 占位"的做法在 senior code base 里很常见。

### `dbt/models/sources.yml`

```yaml
version: 2
sources:
  - name: bronze
    description: "Raw GH Archive events landed by spark/jobs/bronze_ingest.py"
    meta:
      external_location: "{{ env_var('OSS_PULSE_BRONZE_PATH', '../data/bronze/events') }}"
    tables:
      - name: events
        description: "Bronze events table..."
        columns:
          - name: id
            description: "GitHub-issued event id, primary key (ADR-0002)"
            tests:
              - unique
              - not_null
          - name: type
            tests:
              - not_null
          - name: ingest_hour
            description: "Partition column (ADR-0003), format YYYY-MM-DD-HH"
            tests:
              - not_null
```

读这段:

- `sources` 列表里每个 entry 是个上游"数据源"(non-dbt-managed table)
- 我们对 source 也写 schema test:`id unique + not_null`、`type not_null`、`ingest_hour not_null`。**这些 test 在每次 `dbt test` 时跑,如果哪天 Bronze 的 id 出现重复,test 会 fail——这是对 ADR-0002 的持续监控**
- `external_location` 是我们自定义的 meta,被 `register_external_sources` macro 读取

### `dbt/models/silver/events_push.sql`(主角)

```sql
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

with bronze_push as (
    select
        id, actor_id, actor_login, repo_id, repo_name,
        org_id, org_login, is_public, created_at, ingest_hour,
        payload_raw
    from {{ delta_source('bronze', 'events') }}
    where type = 'PushEvent'

    {% if is_incremental() %}
        and ingest_hour > (select coalesce(max(ingest_hour), '1970-01-01-00') from {{ this }})
    {% endif %}
),

parsed as (
    select
        id, actor_id, actor_login, repo_id, repo_name,
        org_id, org_login, is_public, created_at, ingest_hour,
        cast(get_json_object(payload_raw, '$.push_id')       as bigint) as push_id,
        -- Fix for incident-0001: payload.size renamed to payload.commit_count
        coalesce(
            cast(get_json_object(payload_raw, '$.size')         as int),
            cast(get_json_object(payload_raw, '$.commit_count') as int)
        )                                                              as commit_size,
        cast(get_json_object(payload_raw, '$.distinct_size') as int)    as distinct_commit_size,
                get_json_object(payload_raw, '$.ref')                   as ref,
                get_json_object(payload_raw, '$.head')                  as head_sha,
                get_json_object(payload_raw, '$.before')                as before_sha
    from bronze_push
)

select * from parsed
```

逐段:

**Config block(顶部)**:

- `materialized='incremental'`:这个 model 不是每次重建,而是增量。dbt 第一次 build 会建表,后续每次只处理"新的"数据并 merge 进去
- `unique_key='id'`:增量合并时用 `id` 做匹配键
- `incremental_strategy='merge'`:用 Delta MERGE 而不是 INSERT(意味着重复跑同一份输入不会产生重复行——跟 Bronze 一样的幂等故事)
- `file_format='delta'`:用 Delta 格式
- `on_schema_change='fail'`:如果某天我改了 SELECT 的列,build 直接 fail,逼我去 ADR 化这个改动

**`bronze_push` CTE**:

- `from {{ delta_source('bronze', 'events') }} where type = 'PushEvent'`:从 Bronze 选 PushEvent
- `{% if is_incremental() %}` 这一段是 dbt 的 incremental cutoff:**只有这个 model 已经存在时才加这个 filter**,用 `ingest_hour > max(ingest_hour) in target` 实现"只跑新的 partition"

**`parsed` CTE**:

- 把 envelope 字段透传过去
- 把 `payload_raw` 里 PushEvent 特有的字段用 `get_json_object` 抽出来
- 注意 `commit_size` 那一段:`coalesce(size, commit_count)` —— 这是 [Ch 09](09-sprint5b-incident-postmortem.md) 那个故意制造的 incident 教训留下的修复。Sprint 1 阶段最初是 `cast(get_json_object('$.size') as int)`,Sprint 5b 模拟 GitHub 把 `size` 重命名为 `commit_count` 之后,我们改成 coalesce 兼容两个名字
- 没有把 `payload_raw` 透传到 Silver 表里——只解析需要的字段,raw 留在 Bronze。这是 ADR-0001 设计的延续

**最终 `select * from parsed`**:dbt 拿这个结果集去 MERGE 进 `silver.events_push`

### `dbt/models/silver/_silver_schema.yml`(schema test)

```yaml
version: 2

models:
  - name: events_push
    description: |
      Silver: flattened PushEvent fields. ...
    columns:
      - name: id
        description: GitHub event id (primary key, ADR-0002)
        tests:
          - unique
          - not_null
      - name: actor_id
        tests: [not_null]
      - name: push_id
        tests: [not_null]
      - name: commit_size
        tests: [not_null]
      # ...
```

每个 column 跟着一组 declarative test。`dbt test` 命令会:

- 对 `id` 自动生成 `select count(*) from (select id from events_push group by id having count(*) > 1)`,期望 0
- 对每个 not_null 字段生成 `select count(*) from events_push where <col> is null`,期望 0

**重点**:这种 test 不是手写 SQL,是 declarative。dbt 框架帮你生成 SQL。这就是 dbt 比直接 Spark SQL 强的地方之一。

## 验证 — 这阶段怎么知道做对了

```bash
cd dbt
JAVA_HOME=... ../.venv/bin/dbt run --select silver.events_push
# OK created sql incremental model silver.events_push

JAVA_HOME=... ../.venv/bin/dbt test --select silver.events_push
# 9/9 tests pass

# 再 run 一次,期望 0 新行
JAVA_HOME=... ../.venv/bin/dbt run --select silver.events_push
# Done. PASS=2  → 表行数没变(增量 cutoff 起作用了)
```

**Sprint 1 step 4 的真实数据**:Bronze 613,876 行中 385,321 是 PushEvent。Silver `events_push` 应该有 385,321 行。`dbt test` 的 9 个 case 都过。

## 代码 review 笔记

在写这章时复看 `events_push.sql`,我发现一个**很微妙的潜在问题**:

`bronze_push` CTE 透传 `payload_raw` 到 `parsed` CTE,但 `parsed` 的 select 没有 `payload_raw`。Spark/dbt 在 build 时会 prune 这个列(不读取它),所以**没有性能问题**。但读 SQL 的人会困惑:"为什么 CTE 选了 payload_raw 然后又不用?"

更清晰的写法是把 `get_json_object(payload_raw, ...)` 直接放在 `bronze_push` 里,跳过 CTE 中转。

但我**没改**,因为:

1. 现在的两层 CTE 结构(`bronze_push` filter + `parsed` 解析)读起来更分步
2. 给读者一种"先 filter,再 transform"的视觉节奏
3. Spark planner 会自动 prune,不浪费

这是一个 "可读性 vs 极简" 的 trade-off,我选了可读性。面试官问到的话,你可以这么回答。

## You will be able to say

### 2-minute version (English)

> "Sprint 1 step 4 is the first Silver model — `events_push`. It's
> a dbt-spark incremental model that filters Bronze for PushEvent
> and parses the payload JSON into typed columns. Three things make
> it dbt-canonical:
>
> First, `materialized='incremental'` with `unique_key='id'` and
> `incremental_strategy='merge'`. So it inherits Bronze's MERGE-on-id
> idempotency story — re-run the same hour and zero new rows.
>
> Second, `on_schema_change='fail'`. If I accidentally drop a SELECT
> column, dbt build fails instead of silently dropping the column
> from Silver. Explicit beats implicit.
>
> Third, the schema yml has declarative tests: id unique, not_null
> on every critical column. `dbt test` runs nine assertions, all
> pass on 385,321 rows in about two seconds.
>
> The macros — `delta_source` wraps dbt's source(),
> `register_external_sources` is an on-run-start hook that creates
> the `bronze.events` external table at the path from `--vars` or
> env_var. The env_var fallback is how the same code runs locally
> against `data/bronze/events` and on Sprint 5a's cloud target
> against `s3a://oss-pulse-bronze-…/events`."

## 常见尖刻问题 + 准备好的答案

**Q: "你为什么 `on_schema_change='fail'` 而不是 `append_new_columns`?"**

> "Append-new-columns is silent. It hides the fact that the
> upstream contract changed. Fail forces me — or any future maintainer
> — to look at the change, decide if it should propagate to Silver,
> and either update the model and re-run with full-refresh, or
> explicitly drop the column. The 30 seconds of inconvenience is
> worth the explicit contract."

**Q: "如果 Bronze 已经增量摄入了 1 月,Silver 全 refresh 怎么办?"**

> "`dbt run --select silver.events_push --full-refresh` drops and
> rebuilds. It's the documented escape hatch. The cost is one full
> Bronze scan, which on 4 ingest_hours is ~8 seconds; on a full
> year of Bronze it'd be ~30 minutes. Acceptable for rare migrations.
> I also use this exact full-refresh in Sprint 5b's incident drill
> to validate the coalesce fix actually fixed the historic NULLs."

**Q: "`get_json_object` 比 from_json + struct 慢吧?"**

> "Yes, ~30% slower per call. But struct parsing requires defining
> the schema upfront, which violates ADR-0001's schema-drift
> tolerance. The trade is: explicit-but-slow parse at Silver, vs.
> schema-enforced-but-brittle parse at Bronze. I picked the former.
> If a Silver build became a bottleneck, I'd profile to see whether
> get_json_object is actually the hot path — Sprint 5b's bench
> showed the bottleneck is Spark startup, not JSON parse, at the
> current scale."

---

下一章 →  [04-sprint2-first-gold-mart.md](04-sprint2-first-gold-mart.md)
