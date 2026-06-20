"""The bronze ``likes`` table schema (Iceberg + matching pyarrow) and partition spec.

Pure: no I/O. Mirror of ``bronze_schema.py`` for the like event type — the **Iceberg**
``Schema`` is the source of truth and the pyarrow schema mirrors it field-for-field (same
names, types, nullability), because PyIceberg's append refuses a pyarrow batch whose
required/optional or tz-awareness differs from the table.

Columns mirror ``schemas/avro/bsky.likes.v1.avsc`` (and so ``schemas.models.BskyLike``) plus
Kafka provenance the firehose record doesn't carry. Unlike posts, a like has NO nullable
payload fields — every field is required. ``created_at``/``ingest_ts`` are microsecond UTC
timestamps (``timestamptz``); the table is partitioned by day(created_at). ``created_at`` is
kept the 4th field so the shared day-partition field id matches the posts table.
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Column order is the contract the decode + landing layers rely on.
BRONZE_COLUMNS_LIKES: tuple[str, ...] = (
    "did",
    "rkey",
    "cid",
    "created_at",
    "subject_uri",
    "subject_cid",
    "kafka_partition",
    "kafka_offset",
    "ingest_ts",
)

# Field id for the day(created_at) partition; created_at is field 4 (see below).
_CREATED_AT_FIELD_ID = 4


def likes_iceberg_schema() -> Schema:
    """The Iceberg schema (source of truth for the table)."""
    return Schema(
        NestedField(1, "did", StringType(), required=True),
        NestedField(2, "rkey", StringType(), required=True),
        NestedField(3, "cid", StringType(), required=True),
        NestedField(_CREATED_AT_FIELD_ID, "created_at", TimestamptzType(), required=True),
        NestedField(5, "subject_uri", StringType(), required=True),
        NestedField(6, "subject_cid", StringType(), required=True),
        NestedField(7, "kafka_partition", IntegerType(), required=True),
        NestedField(8, "kafka_offset", LongType(), required=True),
        NestedField(9, "ingest_ts", TimestamptzType(), required=True),
    )


def likes_arrow_schema() -> pa.Schema:
    """pyarrow schema for each append batch; mirrors the Iceberg schema exactly."""
    return pa.schema(
        [
            pa.field("did", pa.string(), nullable=False),
            pa.field("rkey", pa.string(), nullable=False),
            pa.field("cid", pa.string(), nullable=False),
            pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("subject_uri", pa.string(), nullable=False),
            pa.field("subject_cid", pa.string(), nullable=False),
            pa.field("kafka_partition", pa.int32(), nullable=False),
            pa.field("kafka_offset", pa.int64(), nullable=False),
            pa.field("ingest_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )


def likes_partition_spec() -> PartitionSpec:
    """Partition the bronze likes table by day(created_at)."""
    return PartitionSpec(
        PartitionField(
            source_id=_CREATED_AT_FIELD_ID,
            field_id=1000,
            transform=DayTransform(),
            name="created_at_day",
        )
    )
