"""Pure decode of a deserialized Kafka post record into a bronze row (or a DLQ row).

No I/O: the Avro byte-decoding (registry lookup) happens in the resource; this function
takes the already-deserialized dict and validates it against ``schemas.models.BskyPost``,
producing a typed ``BronzeRow``. Anything that fails validation becomes a ``DlqRow`` and is
NEVER raised — the caller routes it to ``bsky.dlq.v1`` so a lexicon change degrades to a
quarantined record, never a crashed run or a silent drop. The catch set
(ValidationError, KeyError, TypeError, ValueError) matches ingest/parse.py's DLQ boundary.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from schemas.models import BskyPost


@dataclass(frozen=True, slots=True)
class BronzeRow:
    """One validated post, in BRONZE_COLUMNS order, ready for the pyarrow batch."""

    did: str
    rkey: str
    cid: str
    created_at: datetime
    text: str
    langs: list[str]
    reply_parent: str | None
    reply_root: str | None
    kafka_partition: int
    kafka_offset: int
    ingest_ts: datetime


@dataclass(frozen=True, slots=True)
class DlqRow:
    """An undecodable record, wrapped for ``bsky.dlq.v1``."""

    raw_payload: str
    error: str


def decode_post(
    obj: dict[str, Any] | None,
    *,
    kafka_partition: int,
    kafka_offset: int,
    ingest_ts: datetime,
) -> BronzeRow | DlqRow:
    """Validate one deserialized post dict into a BronzeRow, or a DlqRow on any failure."""
    if obj is None:  # tombstone / undecodable payload
        return DlqRow(raw_payload="null", error="empty or undecodable Avro payload")
    try:
        post = BskyPost.model_validate(obj)
        return BronzeRow(
            did=post.did,
            rkey=post.rkey,
            cid=post.cid,
            created_at=post.created_at,
            text=post.text,
            langs=list(post.langs) if post.langs else [],  # Avro null -> []
            reply_parent=post.reply_parent,
            reply_root=post.reply_root,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            ingest_ts=ingest_ts,
        )
    except (ValidationError, KeyError, TypeError, ValueError) as exc:
        return DlqRow(raw_payload=_safe_json(obj), error=f"{type(exc).__name__}: {exc}")


def _safe_json(obj: Any) -> str:
    """Best-effort JSON of the raw payload for DLQ storage (stable, never raises)."""
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return repr(obj)
