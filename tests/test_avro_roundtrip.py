"""Avro round-trip tests: every model serializes and deserializes through its .avsc.

This verifies the schemas in schemas/avro/ actually match the Pydantic models — most
importantly that datetime maps cleanly through the timestamp-micros logical type. Uses
fastavro directly (the lib confluent-kafka's AvroSerializer uses under the hood); no
Schema Registry needed to prove schema/model fidelity.
"""

from __future__ import annotations

import io
import json
import pathlib
from datetime import UTC, datetime
from typing import Any

import fastavro
import pytest
from pydantic import BaseModel

from schemas.models import BskyFollow, BskyLike, BskyPost, DlqEnvelope

_AVRO_DIR = pathlib.Path(__file__).resolve().parent.parent / "schemas" / "avro"


def _roundtrip(record: dict[str, Any], avsc: str) -> dict[str, Any]:
    schema = fastavro.parse_schema(json.loads((_AVRO_DIR / avsc).read_text()))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)
    assert isinstance(decoded, dict)
    return decoded


def test_post_roundtrip() -> None:
    model = BskyPost(
        did="did:plc:a",
        rkey="r",
        cid="c",
        created_at=datetime(2024, 1, 1, tzinfo=UTC),
        text="hi",
        langs=["en", "ja"],
        reply_parent="at://p",
        reply_root="at://r",
    )
    decoded = _roundtrip(model.model_dump(), "bsky.posts.v1.avsc")
    assert decoded["text"] == "hi"
    assert decoded["langs"] == ["en", "ja"]
    assert decoded["reply_parent"] == "at://p"
    # timestamp-micros decodes back to a tz-aware datetime equal to the original.
    assert decoded["created_at"] == model.created_at


def test_post_roundtrip_with_nulls() -> None:
    model = BskyPost(
        did="did:plc:a", rkey="r", cid="c", created_at=datetime(2024, 1, 1, tzinfo=UTC), text="x"
    )
    decoded = _roundtrip(model.model_dump(), "bsky.posts.v1.avsc")
    assert decoded["langs"] is None
    assert decoded["reply_parent"] is None
    assert decoded["reply_root"] is None


def test_like_roundtrip() -> None:
    model = BskyLike(
        did="did:plc:a",
        rkey="r",
        cid="c",
        created_at=datetime(2024, 1, 1, tzinfo=UTC),
        subject_uri="at://x",
        subject_cid="bafyx",
    )
    decoded = _roundtrip(model.model_dump(), "bsky.likes.v1.avsc")
    assert decoded["subject_uri"] == "at://x"
    assert decoded["subject_cid"] == "bafyx"


def test_follow_roundtrip() -> None:
    model = BskyFollow(
        did="did:plc:a",
        rkey="r",
        cid="c",
        created_at=datetime(2024, 1, 1, tzinfo=UTC),
        subject_did="did:plc:b",
    )
    decoded = _roundtrip(model.model_dump(), "bsky.follows.v1.avsc")
    assert decoded["subject_did"] == "did:plc:b"


def test_dlq_roundtrip() -> None:
    model = DlqEnvelope(
        raw_payload='{"x":1}',
        error="boom",
        intended_topic="bsky.posts.v1",
        received_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    decoded = _roundtrip(model.model_dump(), "bsky.dlq.v1.avsc")
    assert decoded["raw_payload"] == '{"x":1}'
    assert decoded["error"] == "boom"
    assert decoded["intended_topic"] == "bsky.posts.v1"


@pytest.mark.parametrize(
    ("model_cls", "avsc"),
    [
        (BskyPost, "bsky.posts.v1.avsc"),
        (BskyLike, "bsky.likes.v1.avsc"),
        (BskyFollow, "bsky.follows.v1.avsc"),
        (DlqEnvelope, "bsky.dlq.v1.avsc"),
    ],
)
def test_model_fields_match_avro_fields(model_cls: type[BaseModel], avsc: str) -> None:
    schema = json.loads((_AVRO_DIR / avsc).read_text())
    avro_fields = {f["name"] for f in schema["fields"]}
    assert set(model_cls.model_fields) == avro_fields
