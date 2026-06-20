"""Pure decode of a deserialized Kafka like record into a bronze row (or a DLQ row).

Mirror of ``decode.py`` for the like event type. No I/O: takes the already-deserialized dict
and validates it against ``schemas.models.BskyLike``, producing a typed ``LikeRow``. Anything
that fails validation becomes a ``DlqRow`` (reused from ``decode``) and is NEVER raised — the
caller routes it to ``bsky.dlq.v1``. The catch set (ValidationError, KeyError, TypeError,
ValueError) matches ingest/parse.py's DLQ boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from bsky_dagster.transforms.decode import DlqRow, _safe_json
from schemas.models import BskyLike


@dataclass(frozen=True, slots=True)
class LikeRow:
    """One validated like, in BRONZE_COLUMNS_LIKES order, ready for the pyarrow batch."""

    did: str
    rkey: str
    cid: str
    created_at: datetime
    subject_uri: str
    subject_cid: str
    kafka_partition: int
    kafka_offset: int
    ingest_ts: datetime


def decode_like(
    obj: dict[str, Any] | None,
    *,
    kafka_partition: int,
    kafka_offset: int,
    ingest_ts: datetime,
) -> LikeRow | DlqRow:
    """Validate one deserialized like dict into a LikeRow, or a DlqRow on any failure."""
    if obj is None:  # tombstone / undecodable payload
        return DlqRow(raw_payload="null", error="empty or undecodable Avro payload")
    try:
        like = BskyLike.model_validate(obj)
        return LikeRow(
            did=like.did,
            rkey=like.rkey,
            cid=like.cid,
            created_at=like.created_at,
            subject_uri=like.subject_uri,
            subject_cid=like.subject_cid,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            ingest_ts=ingest_ts,
        )
    except (ValidationError, KeyError, TypeError, ValueError) as exc:
        return DlqRow(raw_payload=_safe_json(obj), error=f"{type(exc).__name__}: {exc}")
