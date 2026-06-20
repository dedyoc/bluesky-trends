"""Tests for the bronze follows pyarrow schema + partition spec (avsc parity)."""

from __future__ import annotations

import pyarrow as pa
from bsky_dagster.transforms.bronze_schema_follows import (
    BRONZE_COLUMNS_FOLLOWS,
    follows_arrow_schema,
)


def test_schema_columns_match_contract() -> None:
    schema = follows_arrow_schema()
    assert tuple(schema.names) == BRONZE_COLUMNS_FOLLOWS


def test_schema_types_mirror_avsc() -> None:
    schema = follows_arrow_schema()
    types = {f.name: f.type for f in schema}
    # created_at / ingest_ts are microsecond UTC (avsc timestamp-micros).
    assert types["created_at"] == pa.timestamp("us", tz="UTC")
    assert types["ingest_ts"] == pa.timestamp("us", tz="UTC")
    # subject_did is a plain (required) string — no nullable payload fields on a follow.
    assert types["subject_did"] == pa.string()
    # kafka provenance columns.
    assert types["kafka_partition"] == pa.int32()
    assert types["kafka_offset"] == pa.int64()


def test_all_fields_non_nullable() -> None:
    schema = follows_arrow_schema()
    for name in BRONZE_COLUMNS_FOLLOWS:
        assert schema.field(name).nullable is False, name
