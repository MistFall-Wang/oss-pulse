"""Quick Bronze table inspection."""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from spark.jobs.bronze_ingest import build_spark

BRONZE_PATH = "data/bronze/events"


def main() -> None:
    spark = build_spark("bronze_inspect")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(BRONZE_PATH)

    print("\n=== Schema ===")
    df.printSchema()

    print("\n=== Row counts by ingest_hour ===")
    df.groupBy("ingest_hour").count().orderBy("ingest_hour").show()

    print("\n=== Event type distribution (across all loaded hours) ===")
    df.groupBy("type").count().orderBy(F.col("count").desc()).show(20, truncate=False)

    print("\n=== Sample row (one PushEvent) ===")
    sample_df = df.filter("type = 'PushEvent'").limit(1)
    display_df = sample_df.select(
        *[
            F.date_format(field.name, "yyyy-MM-dd HH:mm:ss").alias(field.name)
            if isinstance(field.dataType, TimestampType)
            else F.col(field.name)
            for field in df.schema.fields
        ]
    )
    sample = display_df.collect()[0]
    for field in df.schema.fields:
        val = sample[field.name]
        if field.name == "payload_raw":
            val = (val[:200] + "...") if val and len(val) > 200 else val
        print(f"  {field.name:18s} = {val}")

    print("\n=== Idempotency invariant ===")
    total = df.count()
    unique = df.select("id").distinct().count()
    print(f"  total = {total:,}")
    print(f"  unique = {unique:,}")
    print(f"  invariant (total == unique): {total == unique}")

    spark.stop()


if __name__ == "__main__":
    main()
