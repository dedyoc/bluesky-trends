"""Tests for the pure bronze decode boundary."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bsky_dagster.transforms.decode import BronzeRow, DlqRow, decode_post

_INGEST_TS = datetime(2026, 6, 18, tzinfo=UTC)


def test_decode_happy(deserialized_post: dict[str, Any]) -> None:
    row = decode_post(deserialized_post, kafka_partition=2, kafka_offset=42, ingest_ts=_INGEST_TS)
    assert isinstance(row, BronzeRow)
    assert row.did == "did:plc:author123"
    assert row.rkey == "3kpostrkey"
    assert row.langs == ["en"]
    assert row.kafka_partition == 2
    assert row.kafka_offset == 42
    assert row.ingest_ts == _INGEST_TS


def test_decode_null_langs_becomes_empty_list(deserialized_post_no_langs: dict[str, Any]) -> None:
    row = decode_post(
        deserialized_post_no_langs, kafka_partition=0, kafka_offset=1, ingest_ts=_INGEST_TS
    )
    assert isinstance(row, BronzeRow)
    assert row.langs == []  # Avro null -> [], never None (matches the v1 ClickHouse mapping)
    assert row.reply_parent is None


def test_decode_missing_required_field_goes_to_dlq(deserialized_post: dict[str, Any]) -> None:
    bad = dict(deserialized_post)
    del bad["text"]  # text is required on BskyPost
    row = decode_post(bad, kafka_partition=1, kafka_offset=7, ingest_ts=_INGEST_TS)
    assert isinstance(row, DlqRow)  # never raises
    assert "text" in row.error or "ValidationError" in row.error
    assert "did:plc:author123" in row.raw_payload


def test_decode_none_payload_goes_to_dlq() -> None:
    row = decode_post(None, kafka_partition=0, kafka_offset=0, ingest_ts=_INGEST_TS)
    assert isinstance(row, DlqRow)
    assert "undecodable" in row.error
