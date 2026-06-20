"""Tests for the pure bronze follow decode boundary."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from bsky_dagster.transforms.decode import DlqRow
from bsky_dagster.transforms.decode_follows import FollowRow, decode_follow

_INGEST_TS = datetime(2026, 6, 18, tzinfo=UTC)


def test_decode_happy(deserialized_follow: dict[str, Any]) -> None:
    row = decode_follow(
        deserialized_follow, kafka_partition=2, kafka_offset=42, ingest_ts=_INGEST_TS
    )
    assert isinstance(row, FollowRow)
    assert row.did == "did:plc:follower123"
    assert row.rkey == "3kfollowkey"
    assert row.subject_did == "did:plc:followed456"
    assert row.kafka_partition == 2
    assert row.kafka_offset == 42
    assert row.ingest_ts == _INGEST_TS


def test_decode_missing_required_field_goes_to_dlq(deserialized_follow: dict[str, Any]) -> None:
    bad = dict(deserialized_follow)
    del bad["subject_did"]  # subject_did is required on BskyFollow
    row = decode_follow(bad, kafka_partition=1, kafka_offset=7, ingest_ts=_INGEST_TS)
    assert isinstance(row, DlqRow)  # never raises
    assert "subject_did" in row.error or "ValidationError" in row.error
    assert "did:plc:follower123" in row.raw_payload


def test_decode_none_payload_goes_to_dlq() -> None:
    row = decode_follow(None, kafka_partition=0, kafka_offset=0, ingest_ts=_INGEST_TS)
    assert isinstance(row, DlqRow)
    assert "undecodable" in row.error
