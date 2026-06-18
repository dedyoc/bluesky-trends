"""Tests for the bronze pyarrow schema + partition spec (avsc parity)."""

from __future__ import annotations

import pyarrow as pa
from bsky_dagster.transforms.bronze_schema import (
    BRONZE_COLUMNS,
    bronze_arrow_schema,
)


def test_schema_columns_match_contract() -> None:
    schema = bronze_arrow_schema()
    assert tuple(schema.names) == BRONZE_COLUMNS


def test_schema_types_mirror_avsc() -> None:
    schema = bronze_arrow_schema()
    types = {f.name: f.type for f in schema}
    # created_at / ingest_ts are microsecond UTC (avsc timestamp-micros).
    assert types["created_at"] == pa.timestamp("us", tz="UTC")
    assert types["ingest_ts"] == pa.timestamp("us", tz="UTC")
    # langs is a non-nullable list (Avro null -> []), not Nullable.
    assert types["langs"] == pa.list_(pa.string())
    assert schema.field("langs").nullable is False
    # reply fields are nullable strings.
    assert schema.field("reply_parent").nullable is True
    assert schema.field("reply_root").nullable is True
    # kafka provenance columns.
    assert types["kafka_partition"] == pa.int32()
    assert types["kafka_offset"] == pa.int64()


def test_required_identity_fields_non_nullable() -> None:
    schema = bronze_arrow_schema()
    for name in ("did", "rkey", "cid", "created_at", "text"):
        assert schema.field(name).nullable is False, name
