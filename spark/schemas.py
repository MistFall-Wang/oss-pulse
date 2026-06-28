"""Bronze table schema constants.

Single source of truth for the envelope schema documented in ADR-0001.
Envelope columns are strongly typed; payload lands as raw JSON string.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_EVENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("type", StringType(), nullable=False),
        StructField("actor_id", LongType(), nullable=False),
        StructField("actor_login", StringType(), nullable=False),
        StructField("repo_id", LongType(), nullable=False),
        StructField("repo_name", StringType(), nullable=False),
        StructField("org_id", LongType(), nullable=True),
        StructField("org_login", StringType(), nullable=True),
        StructField("is_public", BooleanType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("created_at_raw", StringType(), nullable=False),
        StructField("payload_raw", StringType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("ingest_hour", StringType(), nullable=False),
        StructField("ingest_run_id", StringType(), nullable=False),
    ]
)
