"""The bronze ``posts`` table schema (Iceberg + matching pyarrow) and partition spec.

Pure: no I/O. The **Iceberg** ``Schema`` is the source of truth for the table; the pyarrow
schema (used to shape each append batch) mirrors it field-for-field — same names, types,
and nullability — because PyIceberg's append refuses a pyarrow batch whose required/optional
or tz-awareness differs from the table.

Columns mirror ``schemas/avro/bsky.posts.v1.avsc`` (and so ``schemas.models.BskyPost``) plus
Kafka provenance the firehose record doesn't carry. ``langs`` is a required list with
optional elements (Avro null -> empty list, matching the v1 ClickHouse mapping).
``created_at``/``ingest_ts`` are microsecond UTC timestamps (``timestamptz``; the avsc
timestamp-micros logical type). The table is partitioned by day(created_at) — the analogue
of the v1 mart's ``PARTITION BY toYYYYMMDD(created_at)``.
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Column order is the contract the decode + landing layers rely on.
BRONZE_COLUMNS: tuple[str, ...] = (
    "did",
    "rkey",
    "cid",
    "created_at",
    "text",
    "langs",
    "reply_parent",
    "reply_root",
    "kafka_partition",
    "kafka_offset",
    "ingest_ts",
)

# Field id for the day(created_at) partition; created_at is field 4 (see below).
_CREATED_AT_FIELD_ID = 4


def bronze_iceberg_schema() -> Schema:
    """The Iceberg schema (source of truth for the table)."""
    return Schema(
        NestedField(1, "did", StringType(), required=True),
        NestedField(2, "rkey", StringType(), required=True),
        NestedField(3, "cid", StringType(), required=True),
        NestedField(_CREATED_AT_FIELD_ID, "created_at", TimestamptzType(), required=True),
        NestedField(5, "text", StringType(), required=True),
        NestedField(
            6,
            "langs",
            ListType(element_id=100, element_type=StringType(), element_required=False),
            required=True,
        ),
        NestedField(7, "reply_parent", StringType(), required=False),
        NestedField(8, "reply_root", StringType(), required=False),
        NestedField(9, "kafka_partition", IntegerType(), required=True),
        NestedField(10, "kafka_offset", LongType(), required=True),
        NestedField(11, "ingest_ts", TimestamptzType(), required=True),
    )


def bronze_arrow_schema() -> pa.Schema:
    """pyarrow schema for each append batch; mirrors the Iceberg schema exactly."""
    return pa.schema(
        [
            pa.field("did", pa.string(), nullable=False),
            pa.field("rkey", pa.string(), nullable=False),
            pa.field("cid", pa.string(), nullable=False),
            pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("text", pa.string(), nullable=False),
            pa.field("langs", pa.list_(pa.string()), nullable=False),
            pa.field("reply_parent", pa.string(), nullable=True),
            pa.field("reply_root", pa.string(), nullable=True),
            pa.field("kafka_partition", pa.int32(), nullable=False),
            pa.field("kafka_offset", pa.int64(), nullable=False),
            pa.field("ingest_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )


def partition_spec() -> PartitionSpec:
    """Partition the bronze table by day(created_at)."""
    return PartitionSpec(
        PartitionField(
            source_id=_CREATED_AT_FIELD_ID,
            field_id=1000,
            transform=DayTransform(),
            name="created_at_day",
        )
    )
