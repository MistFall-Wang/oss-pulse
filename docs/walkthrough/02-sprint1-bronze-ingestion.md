# Chapter 02 — Sprint 1:Bronze 摄入

## 这章你会学到什么

把 GH Archive 的 `.json.gz` 文件读进来、变成 Delta 表的全过程。读完你能解释:**MERGE on event_id 是什么、为什么这个项目把幂等性放在第一位、partition by ingest_hour 为什么比 partition by date 好**。这章对应项目里两个 ADR(0002 和 0003)和最重要的一个 Spark 脚本。

## 关联前后

- **上一章** ([Ch 01](01-sprint0-schema-discovery.md)) 决定了 Bronze schema 应该长什么样(envelope 强类型,payload 存 STRING)
- **下一章** ([Ch 03](03-sprint1-first-silver.md)) 在这个 Bronze 之上建第一个 Silver 模型

## 背景概念(30 秒补课)

- **幂等(Idempotency)**:同样的输入跑多次,结果一样。比如把同一份文件摄入两次,表里事件总数应该跟摄入一次相同(没有重复行)。这是数据管线最基本的合约。
- **Spark `SparkSession`**:一个 PySpark 程序的入口。`local[*]` 表示用本机所有 CPU 核当 worker;真上集群时换成 `yarn` 或 cluster URL。
- **Delta MERGE**:类似 SQL 的 `MERGE INTO`,根据某个 key 来"匹配则更新 / 不匹配则插入"。比 INSERT 安全,因为重复跑不会 dup。
- **Partition**:Delta 表(底下是 parquet 文件)按某列拆成子目录,例如 `ingest_hour=2025-01-15-12/`,这样查询时只读相关目录,跳过其他。
- **ZORDER**:Delta 在同一个分区内,按某列把行重排到一起,这样下次按那列 filter 时,可以 skip 掉很多 file。注意 ZORDER 不创建新分区,是文件内的 row 排序。
- **`_delta_log/`**:Delta 表元数据所在目录。每次写操作产生一个 commit JSON,这就是 Delta 实现 ACID 事务的方式。

## 这一阶段的目标

把 4 小时的 GH Archive 数据(`2015-01-15-12`, `2018-01-15-12`, `2025-01-15-12`, `2025-01-15-13`)摄入到本地 Delta 表 `data/bronze/events/`。要求:

1. 重复跑同一份文件,表行数不变(幂等)
2. 按 `ingest_hour` 物理分区
3. envelope 字段强类型,`payload` 字段保留 raw JSON STRING
4. 每行带 `ingest_run_id` 用来溯源(哪次摄入产生了这行)

## 设计决策怎么做的

### 决策 1:用什么做 idempotency key

**候选方案**:

| 选项 | 优 | 劣 |
|------|----|----|
| (a) 用文件名 hash + line offset | 不依赖数据本身 | 同一份数据放在不同路径 = 不同 key,fails 重新摄入场景 |
| (b) (ingest_hour, event_id) 复合 | 范围明确 | 同一个 event_id 跨 hour 怎么办?(不应该跨,但要确保) |
| (c) 单独 event_id | 简单、跟 GitHub 自己的 id 对齐 | 信任 GitHub 的 id 唯一性 |

选 (c)。证据来自 Sprint 0:613,876 行实测 0 重复 event_id。GitHub 文档也明确说 event_id 全局唯一不重用。这是 **ADR-0002**。

### 决策 2:用什么做 partition

**候选方案**:

| 选项 | 优 | 劣 |
|------|----|----|
| (a) `created_at` 的 date 部分 | 跟业务日期对齐 | 摄入时同一个 ingest 文件可能跨 date,partition 计算复杂 |
| (b) `type`(event 类型) | Silver 模型按 type filter,直接 skip 整个 partition | 类型只有 15 种,partition 太宽 |
| (c) `ingest_hour`(从文件名抽取) | 摄入时就知道,不依赖数据;一份文件 → 一个 partition | 查"某天的 event"要扫多个 partition |

选 (c)。**ADR-0003** 给出理由:

- 摄入时只需要文件名就能定 partition,代码简单
- Backfill 时可以按 partition 删除/重写,粒度合适
- 查询按时间 filter 时,先靠 `ingest_hour` partition prune,再靠 ZORDER `created_at` 文件内 skip,双重 prune

### 决策 3:Bronze 写完之后要不要 OPTIMIZE / ZORDER

Sprint 1 阶段:**不做**。Sprint 5b 性能调优时实测过 ZORDER 在 4 partition 规模下没有效果(详见 Ch 08)。这是诚实的设计——不做没用的事。

## 代码逐行讲 — `spark/jobs/bronze_ingest.py`

打开 [`spark/jobs/bronze_ingest.py`](../../spark/jobs/bronze_ingest.py),整个文件分成 6 个函数。

### 1) `build_spark()`:启动 Spark Session

```python
def build_spark(app_name: str = "bronze_ingest") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
```

逐句:

- `master("local[*]")`:本地用所有核。生产换成 `yarn` 或省略
- `spark.sql.extensions`:告诉 Spark "你认识 Delta 这个表格式"
- `spark.sql.catalog.spark_catalog`:把 Spark 默认 catalog 切到 DeltaCatalog,这样 `CREATE TABLE ... USING delta` 才能工作
- `spark.sql.shuffle.partitions=8`:shuffle 后产出 8 个文件。默认 200 太多,小数据集会变成几 KB 一个文件。这是性能调优的预先决定
- `spark.sql.session.timeZone=UTC`:GH Archive 的 timestamp 是 UTC,我们也用 UTC,避免本地时区导致的偏移
- `spark.driver.host=127.0.0.1`:macOS 在某些网络环境下 Spark 启动会卡 hostname resolution,显式指定 loopback 解决
- `configure_spark_with_delta_pip(builder)`:Delta 提供的 helper,自动把 delta jar 加到 classpath

### 2) `extract_ingest_hour()`:从文件名提取 partition 列

```python
_HOUR_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}-\d{1,2})\.json\.gz$")

def extract_ingest_hour(source_path: str) -> str:
    name = Path(source_path).name
    match = _HOUR_PATTERN.search(name)
    if not match:
        raise ValueError(f"cannot extract ingest_hour from {name!r}")
    return match.group(1)
```

为什么要单独写这个函数?**因为它是 pure function,可以单独写单元测试**。打开 `spark/tests/test_bronze_ingest.py`,有两个 case:

```python
def test_extract_ingest_hour_from_gh_archive_filename():
    assert extract_ingest_hour("data/raw/2025-01-15-12.json.gz") == "2025-01-15-12"
    assert extract_ingest_hour("/tmp/2015-01-15-3.json.gz") == "2015-01-15-3"

def test_extract_ingest_hour_rejects_unexpected_filename():
    with pytest.raises(ValueError, match="cannot extract ingest_hour"):
        extract_ingest_hour("data/raw/not-gh-archive.json.gz")
```

**senior signal**:把 IO-free 的逻辑挑出来单独测,这样 CI 不需要 Spark 也能跑。

### 3) `read_raw_events()`:读 JSON.gz 当文本

```python
def read_raw_events(spark: SparkSession, source_path: str) -> DataFrame:
    return (
        spark.read.option("compression", "gzip")
        .text(source_path)
        .withColumnRenamed("value", "raw_line")
    )
```

**关键决策**:用 `.text()` 而不是 `.json()`。为什么?

- `.json()` 会自动 infer schema,十几年的 schema drift 后 infer 出来的类型不稳定
- `.text()` 把每行当字符串。我们手动用 `get_json_object()` 抽取需要的字段,完全控制每个字段的类型

这呼应了 Ch 01 的 ADR-0001:Bronze 的 envelope 强类型,但解析过程是我们写代码控制的,不是 Spark 帮我们 infer 的。

### 4) `shape_to_bronze()`:把 raw 文本变成 Bronze 行

```python
def shape_to_bronze(raw, source_file, ingest_hour, ingest_run_id) -> DataFrame:
    df = (
        raw.select(
            F.get_json_object("raw_line", "$.id").alias("id"),
            F.get_json_object("raw_line", "$.type").alias("type"),
            F.get_json_object("raw_line", "$.actor.id").cast("long").alias("actor_id"),
            F.get_json_object("raw_line", "$.actor.login").alias("actor_login"),
            F.get_json_object("raw_line", "$.repo.id").cast("long").alias("repo_id"),
            F.get_json_object("raw_line", "$.repo.name").alias("repo_name"),
            F.get_json_object("raw_line", "$.org.id").cast("long").alias("org_id"),
            F.get_json_object("raw_line", "$.org.login").alias("org_login"),
            F.get_json_object("raw_line", "$.public").cast("boolean").alias("is_public"),
            F.get_json_object("raw_line", "$.created_at").alias("created_at_raw"),
            F.get_json_object("raw_line", "$.payload").alias("payload_raw"),
        )
        .withColumn("created_at",
                    F.to_timestamp("created_at_raw", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingest_hour", F.lit(ingest_hour))
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
    )
    column_order = [field.name for field in BRONZE_EVENTS_SCHEMA.fields]
    return df.select(*column_order)
```

逐项:

- 8 个 envelope 字段用 `get_json_object` 抽出来,4 个数字字段 `.cast("long")` 显式强转
- `is_public` 强转 boolean(虽然根据 ADR-0001 contract 应该永远是 true,但 cast 保证类型对)
- `payload` 字段保留为 JSON STRING(关键!)叫 `payload_raw`,名字暗示"这是 raw,不是 parsed"
- `created_at_raw` 是字符串,`created_at` 是 parse 后的 timestamp。**两个都留着**,因为后面 debugging 时原始字符串值有用
- 三个 lineage 列:`source_file`(谁产生的)、`ingest_hour`(partition 列)、`ingest_run_id`(UUID,这次摄入运行的标识——同一份文件被摄入两次,会有两个不同的 run_id;但因为 MERGE 去重,只有第一次的 run_id 留在表里)
- 最后 `df.select(*column_order)` 是为了**保证列顺序跟 schema 定义一致**。Spark 写 Delta 表对 schema 强校验,顺序错就失败

### 5) `write_bronze()`:第一次创建 / 后续 MERGE

```python
def write_bronze(spark, batch, bronze_path):
    table_path = Path(bronze_path)
    if not (table_path / "_delta_log").exists():
        # First write: create the table
        batch.write.format("delta") \
             .partitionBy("ingest_hour") \
             .mode("overwrite").save(bronze_path)
        return

    # Subsequent writes: MERGE
    target = DeltaTable.forPath(spark, bronze_path)
    (
        target.alias("t")
        .merge(batch.alias("s"), "t.id = s.id")
        .whenNotMatchedInsertAll()
        .execute()
    )
```

**关键设计**:

- 第一次写(`_delta_log/` 不存在)用 `mode("overwrite") + partitionBy`,这是 Delta 唯一允许指定 partition 的时机
- 后续每次写用 MERGE on `id`。MERGE 的语义:在 target(已有表)和 source(新 batch)按 `t.id = s.id` 匹配
  - `whenNotMatchedInsertAll()`:source 里有但 target 里没有的行,全字段插入
  - **没有 `whenMatchedUpdateAll()`**:source 里有 target 也有的行,**不更新**——这就是幂等性的根本保证

**面试官问 "如果 GitHub 修了某个事件的字段,Bronze 怎么办?"** 答:Bronze 是 immutable log,不更新已有行。如果上游真改了字段,我们看到的是 source 里改后的字段值,但 target 已有的旧值保留——这是 audit trail 的诚实表达。要修业务字段值,该 Silver 重算,不该回 Bronze。

### 6) `main()`:CLI 入口 + 验证

```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--bronze-path", default="data/bronze/events")
    args = parser.parse_args()

    ingest_hour = extract_ingest_hour(args.source)
    ingest_run_id = str(uuid.uuid4())
    source_file = str(Path(args.source).resolve())

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_raw_events(spark, args.source)
    batch = shape_to_bronze(raw, source_file=source_file,
                            ingest_hour=ingest_hour, ingest_run_id=ingest_run_id)

    in_count = batch.count()
    print(f"[ingest] events in batch: {in_count:,}")

    write_bronze(spark, batch, args.bronze_path)

    # Verify invariant after every write
    bronze = spark.read.format("delta").load(args.bronze_path)
    total = bronze.count()
    unique = bronze.select("id").distinct().count()
    print(f"[verify] total bronze rows:   {total:,}")
    print(f"[verify] unique bronze ids:   {unique:,}")
    print(f"[verify] invariant (total == unique): {total == unique}")

    spark.stop()
```

**每次摄入之后自动跑一遍 `count(*) == count(distinct id)`** ——这是项目的核心 invariant。如果某天这个 print 输出 `False`,我们就知道 idempotency 假设崩了,需要 debug。

## 验证 — 这阶段怎么知道做对了

```bash
# 第一次摄入
uv run python -m spark.jobs.bronze_ingest \
  --source data/raw/2025-01-15-12.json.gz \
  --bronze-path data/bronze/events
# [verify] invariant (total == unique): True

# 把同一个文件再摄入一遍
uv run python -m spark.jobs.bronze_ingest \
  --source data/raw/2025-01-15-12.json.gz \
  --bronze-path data/bronze/events
# 重要:表行数没变!
```

这就是"用一行 print 验证幂等"。

## 代码 review 笔记

(我做这章时复看 bronze_ingest.py,发现一处可以更清晰)

`write_bronze` 的"第一次 vs 后续"判断用的是文件系统检查(`_delta_log/` 是否存在)。这在 99% 情况没问题,但有个 corner case:如果有人手动 `rm -rf` 了 `_delta_log/` 但留着 parquet 文件,这函数会"重新创建表",把所有旧数据当 brand-new。

**真正生产代码的健壮做法**:用 `DeltaTable.isDeltaTable(spark, path)`,这个 API 既检查 `_delta_log/` 也校验它是合法的 Delta log。

但在这个 portfolio 里我们没改——理由有二:

1. corner case 在 portfolio 阶段不会出现(没有"恶意 rm -rf 部分文件"的攻击者)
2. 这个 imperfection 反而是面试时一个**好的诚实回答机会**:"如果 reviewer 问 `if not _delta_log.exists()` 是不是不够健壮,我说对——production 应该用 DeltaTable.isDeltaTable,但 portfolio 里我决定不做这个 hardening,因为它不在 senior signal 矩阵上。"

承认局限本身也是 senior signal。

## You will be able to say

### 2-minute version (English)

> "Sprint 1's Bronze ingest is in `spark/jobs/bronze_ingest.py`.
> Six functions, plain Python, no framework. The shape:
>
> 1. Read GH Archive `.json.gz` as text — not JSON — so I control
>    type inference instead of Spark guessing.
> 2. Use `get_json_object` to project the 8 envelope fields with
>    explicit casts. Payload stays a STRING, per ADR-0001.
> 3. Three lineage columns get added: source_file, ingest_hour
>    derived from the filename, and a UUID ingest_run_id.
> 4. First write creates the Delta table partitioned by ingest_hour.
>    Every subsequent write is a MERGE on the GitHub event id, with
>    no UPDATE branch — that's the idempotency guarantee. Re-running
>    the same hour produces zero new rows.
> 5. After every write, `count(*)` and `count(distinct id)` are
>    printed. Running invariant; if they ever diverge, I'd know
>    ADR-0002 broke.
>
> ADR-0002 is the event_id-as-key decision. I checked 613K rows
> across three sample years — zero duplicates. ADR-0003 is the
> partition-by-ingest_hour decision. I considered partition by
> created_at date or by event type, but ingest_hour is what the
> filename gives me at landing time, with no extra computation, so
> backfill grain matches storage grain."

## 常见尖刻问题 + 准备好的答案

**Q: "你只测了 613K 行,怎么知道 event_id 全局唯一?"**

> "I don't know it from my own data. GitHub's API docs guarantee
> event ids are immutable and globally unique. My 613K sample
> isn't proving it — it's the dataset-side validation that GH
> Archive matches that documented contract. If it failed on my
> sample, I'd have known the documented contract was broken
> before building the pipeline."

**Q: "为什么不 partition by date 而 partition by ingest_hour?"**

> "Date requires DATE(created_at) — that's a per-row computation
> Spark has to do during shuffle when writing. Ingest_hour comes
> from the filename, available at the driver before any data is
> read. So partition assignment is essentially free. The trade-off:
> querying 'all events on January 15' has to scan 24 partitions
> instead of 1, but my workload pattern is 'reprocess one hour' not
> 'analyze one date'. If that workload changes, ADR-0003 has a
> revisit clause."

**Q: "MERGE 太慢了,为什么不直接 APPEND?"**

> "MERGE is slower per-write but eliminates 'maybe I already
> ingested this' bookkeeping. With APPEND, every backfill needs
> a deduplication step. MERGE makes idempotency the table's
> property, not the caller's responsibility. For Bronze write
> volume (~150 K rows/hour), the MERGE cost is negligible. At a
> million rows/sec, I'd reconsider — but I'd also stop using
> Spark for ingest at that scale."

---

下一章 →  [03-sprint1-first-silver.md](03-sprint1-first-silver.md)
