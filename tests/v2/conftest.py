"""Fixtures for v2 transform tests.

The bronze decoder operates on an ALREADY-deserialized Avro record (the flat BskyPost
shape the AvroDeserializer yields), not the nested Jetstream event the ingest fixtures
hold. So these fixtures are flat post dicts mirroring schemas/avro/bsky.posts.v1.avsc.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest


@pytest.fixture
def deserialized_post() -> dict[str, Any]:
    """A valid deserialized post record (langs present, reply fields set)."""
    return {
        "did": "did:plc:author123",
        "rkey": "3kpostrkey",
        "cid": "bafypost",
        "created_at": datetime(2024, 1, 1, tzinfo=UTC),
        "text": "hello bluesky",
        "langs": ["en"],
        "reply_parent": "at://did:plc:other/app.bsky.feed.post/parent",
        "reply_root": "at://did:plc:other/app.bsky.feed.post/root",
    }


@pytest.fixture
def deserialized_post_no_langs() -> dict[str, Any]:
    """A valid post with langs=None (Avro null) — must decode to an empty list."""
    return {
        "did": "did:plc:author456",
        "rkey": "3knolangs",
        "cid": "bafynolangs",
        "created_at": datetime(2024, 1, 2, tzinfo=UTC),
        "text": "no langs here",
        "langs": None,
        "reply_parent": None,
        "reply_root": None,
    }
