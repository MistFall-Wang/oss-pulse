from __future__ import annotations

import pytest
from pyspark.sql.types import LongType, StringType

from spark.jobs.bronze_ingest import extract_ingest_hour
from spark.schemas import BRONZE_EVENTS_SCHEMA


def test_extract_ingest_hour_from_gh_archive_filename() -> None:
    assert extract_ingest_hour("data/raw/2025-01-15-12.json.gz") == "2025-01-15-12"
    assert extract_ingest_hour("/tmp/2015-01-15-3.json.gz") == "2015-01-15-3"


def test_extract_ingest_hour_rejects_unexpected_filename() -> None:
    with pytest.raises(ValueError, match="cannot extract ingest_hour"):
        extract_ingest_hour("data/raw/not-gh-archive.json.gz")


def test_bronze_schema_column_order_and_contract() -> None:
    assert [field.name for field in BRONZE_EVENTS_SCHEMA.fields] == [
        "id",
        "type",
        "actor_id",
        "actor_login",
        "repo_id",
        "repo_name",
        "org_id",
        "org_login",
        "is_public",
        "created_at",
        "created_at_raw",
        "payload_raw",
        "source_file",
        "ingest_hour",
        "ingest_run_id",
    ]

    fields = {field.name: field for field in BRONZE_EVENTS_SCHEMA.fields}
    assert isinstance(fields["id"].dataType, StringType)
    assert isinstance(fields["actor_id"].dataType, LongType)
    assert fields["org_id"].nullable is True
    assert fields["payload_raw"].nullable is False
