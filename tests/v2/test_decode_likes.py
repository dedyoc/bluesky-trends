"""Tests for the pure bronze like decode boundary."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bsky_dagster.transforms.decode import DlqRow
from bsky_dagster.transforms.decode_likes import LikeRow, decode_like

_INGEST_TS = datetime(2026, 6, 18, tzinfo=UTC)


def test_decode_happy(deserialized_like: dict[str, Any]) -> None:
    row = decode_like(deserialized_like, kafka_partition=2, kafka_offset=42, ingest_ts=_INGEST_TS)
    assert isinstance(row, LikeRow)
    assert row.did == "did:plc:liker123"
    assert row.rkey == "3klikerkey"
    assert row.subject_uri == "at://did:plc:poster/app.bsky.feed.post/3kpostrkey"
    assert row.subject_cid == "bafypost"
    assert row.kafka_partition == 2
    assert row.kafka_offset == 42
    assert row.ingest_ts == _INGEST_TS


def test_decode_missing_required_field_goes_to_dlq(deserialized_like: dict[str, Any]) -> None:
    bad = dict(deserialized_like)
    del bad["subject_uri"]  # subject_uri is required on BskyLike
    row = decode_like(bad, kafka_partition=1, kafka_offset=7, ingest_ts=_INGEST_TS)
    assert isinstance(row, DlqRow)  # never raises
    assert "subject_uri" in row.error or "ValidationError" in row.error
    assert "did:plc:liker123" in row.raw_payload


def test_decode_none_payload_goes_to_dlq() -> None:
    row = decode_like(None, kafka_partition=0, kafka_offset=0, ingest_ts=_INGEST_TS)
    assert isinstance(row, DlqRow)
    assert "undecodable" in row.error
