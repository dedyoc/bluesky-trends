"""Pure decode of a deserialized Kafka follow record into a bronze row (or a DLQ row).

Mirror of ``decode.py`` for the follow event type. No I/O: takes the already-deserialized
dict and validates it against ``schemas.models.BskyFollow``, producing a typed ``FollowRow``.
Anything that fails validation becomes a ``DlqRow`` (reused from ``decode``) and is NEVER
raised — the caller routes it to ``bsky.dlq.v1``. The catch set (ValidationError, KeyError,
TypeError, ValueError) matches ingest/parse.py's DLQ boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from bsky_dagster.transforms.decode import DlqRow, _safe_json
from schemas.models import BskyFollow


@dataclass(frozen=True, slots=True)
class FollowRow:
    """One validated follow, in BRONZE_COLUMNS_FOLLOWS order, ready for the pyarrow batch."""

    did: str
    rkey: str
    cid: str
    created_at: datetime
    subject_did: str
    kafka_partition: int
    kafka_offset: int
    ingest_ts: datetime


def decode_follow(
    obj: dict[str, Any] | None,
    *,
    kafka_partition: int,
    kafka_offset: int,
    ingest_ts: datetime,
) -> FollowRow | DlqRow:
    """Validate one deserialized follow dict into a FollowRow, or a DlqRow on any failure."""
    if obj is None:  # tombstone / undecodable payload
        return DlqRow(raw_payload="null", error="empty or undecodable Avro payload")
    try:
        follow = BskyFollow.model_validate(obj)
        return FollowRow(
            did=follow.did,
            rkey=follow.rkey,
            cid=follow.cid,
            created_at=follow.created_at,
            subject_did=follow.subject_did,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            ingest_ts=ingest_ts,
        )
    except (ValidationError, KeyError, TypeError, ValueError) as exc:
        return DlqRow(raw_payload=_safe_json(obj), error=f"{type(exc).__name__}: {exc}")
