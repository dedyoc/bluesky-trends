"""Tests for the bronze likes pyarrow schema + partition spec (avsc parity)."""

from __future__ import annotations

import pyarrow as pa
from bsky_dagster.transforms.bronze_schema_likes import (
    BRONZE_COLUMNS_LIKES,
    likes_arrow_schema,
)


def test_schema_columns_match_contract() -> None:
    schema = likes_arrow_schema()
    assert tuple(schema.names) == BRONZE_COLUMNS_LIKES


def test_schema_types_mirror_avsc() -> None:
    schema = likes_arrow_schema()
    types = {f.name: f.type for f in schema}
    # created_at / ingest_ts are microsecond UTC (avsc timestamp-micros).
    assert types["created_at"] == pa.timestamp("us", tz="UTC")
    assert types["ingest_ts"] == pa.timestamp("us", tz="UTC")
    # subject fields are plain (required) strings — no nullable payload fields on a like.
    assert types["subject_uri"] == pa.string()
    assert types["subject_cid"] == pa.string()
    # kafka provenance columns.
    assert types["kafka_partition"] == pa.int32()
    assert types["kafka_offset"] == pa.int64()


def test_all_fields_non_nullable() -> None:
    schema = likes_arrow_schema()
    for name in BRONZE_COLUMNS_LIKES:
        assert schema.field(name).nullable is False, name
