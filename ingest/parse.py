"""Pure transforms: Jetstream commit event -> typed schema model.

No I/O here. A Jetstream "commit" event for a create operation looks like::

    {
      "did": "did:plc:...",
      "time_us": 1700000000000000,
      "kind": "commit",
      "commit": {
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "3k...",
        "cid": "bafy...",
        "record": { "$type": "app.bsky.feed.post", "createdAt": "...", "text": "..." }
      }
    }

``to_model`` raises ``ValueError`` for shapes we don't ingest (non-commit, non-create,
unknown collection) and lets ``pydantic.ValidationError`` propagate for malformed records
so the caller can route to the DLQ.
"""

from __future__ import annotations

from typing import Any

from schemas.models import BskyFollow, BskyLike, BskyPost

Event = BskyPost | BskyLike | BskyFollow

_COLLECTION_TOPIC_KEY = "_topic"


def _common(did: str, commit: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    return {
        "did": did,
        "rkey": commit["rkey"],
        "cid": commit["cid"],
        "created_at": record["createdAt"],
    }


def to_model(event: dict[str, Any]) -> Event:
    """Convert a Jetstream commit/create event into a typed model.

    Raises ValueError if the event is not an ingestable create commit, or if a
    required structural field (commit/collection/record) is missing.
    """
    if event.get("kind") != "commit":
        raise ValueError(f"not a commit event: kind={event.get('kind')!r}")

    commit = event.get("commit")
    if not isinstance(commit, dict):
        raise ValueError("commit event missing 'commit' object")

    if commit.get("operation") != "create":
        raise ValueError(f"unsupported operation: {commit.get('operation')!r}")

    record = commit.get("record")
    if not isinstance(record, dict):
        raise ValueError("commit missing 'record' object")

    did = event["did"]
    collection = commit.get("collection")

    if collection == "app.bsky.feed.post":
        return BskyPost(
            **_common(did, commit, record),
            text=record["text"],
            langs=record.get("langs"),
            reply_parent=_reply(record, "parent"),
            reply_root=_reply(record, "root"),
        )
    if collection == "app.bsky.feed.like":
        subject = record["subject"]
        return BskyLike(
            **_common(did, commit, record),
            subject_uri=subject["uri"],
            subject_cid=subject["cid"],
        )
    if collection == "app.bsky.graph.follow":
        return BskyFollow(
            **_common(did, commit, record),
            subject_did=record["subject"],
        )

    raise ValueError(f"unknown collection: {collection!r}")


def _reply(record: dict[str, Any], side: str) -> str | None:
    reply = record.get("reply")
    if not isinstance(reply, dict):
        return None
    ref = reply.get(side)
    if not isinstance(ref, dict):
        return None
    uri = ref.get("uri")
    return uri if isinstance(uri, str) else None
