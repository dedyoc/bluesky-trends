"""Typed event models — the single source of truth for Kafka-bound shapes.

Every event crossing the ingest -> Kafka boundary is one of these models. No ad-hoc
dicts. Each model has a 1:1 Avro counterpart under ``schemas/avro/``; field names and
types must stay in lockstep with the ``.avsc`` files.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class _Event(BaseModel):
    """Common identity fields shared by all Bluesky record events.

    ``did`` is the repo (author) DID and is used as the Kafka partition key for
    partition affinity. ``(did, rkey)`` together uniquely identify a record;
    downstream sinks dedupe on this pair plus ``cid``.
    """

    model_config = ConfigDict(extra="forbid")

    did: str = Field(description="Author repo DID; used as Kafka partition key.")
    rkey: str = Field(description="Record key within the author's repo.")
    cid: str = Field(description="Content identifier of the record.")
    created_at: datetime = Field(description="Record createdAt timestamp (event time).")


class BskyPost(_Event):
    """A ``app.bsky.feed.post`` create event -> topic ``bsky.posts.v1``."""

    text: str
    langs: list[str] | None = None
    reply_parent: str | None = None
    reply_root: str | None = None


class BskyLike(_Event):
    """A ``app.bsky.feed.like`` create event -> topic ``bsky.likes.v1``."""

    subject_uri: str
    subject_cid: str


class BskyFollow(_Event):
    """A ``app.bsky.graph.follow`` create event -> topic ``bsky.follows.v1``."""

    subject_did: str


class DlqEnvelope(BaseModel):
    """Wrapper for events that failed validation -> topic ``bsky.dlq.v1``.

    Never drop a malformed event silently; wrap the raw payload plus context so the
    DLQ can be inspected and replayed after a lexicon change.
    """

    model_config = ConfigDict(extra="forbid")

    raw_payload: str = Field(description="JSON-serialized original Jetstream event.")
    error: str = Field(description="Validation or processing error message.")
    intended_topic: str = Field(description="Topic the event would have gone to.")
    received_at: datetime = Field(description="When ingest received the event.")
