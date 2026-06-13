"""Tests for the pure Jetstream-event -> typed-model transforms."""

from __future__ import annotations

from typing import Any

import pytest

from ingest.parse import to_model
from schemas.models import BskyFollow, BskyLike, BskyPost


def test_parse_post_happy(post_event: dict[str, Any]) -> None:
    model = to_model(post_event)
    assert isinstance(model, BskyPost)
    assert model.did == "did:plc:author123"
    assert model.rkey == "3kpostrkey"
    assert model.cid == "bafypost"
    assert model.text == "hello bluesky"
    assert model.langs == ["en"]
    assert model.reply_parent == "at://did:plc:other/app.bsky.feed.post/parent"
    assert model.reply_root == "at://did:plc:other/app.bsky.feed.post/root"


def test_parse_like_happy(like_event: dict[str, Any]) -> None:
    model = to_model(like_event)
    assert isinstance(model, BskyLike)
    assert model.subject_uri == "at://did:plc:other/app.bsky.feed.post/liked"
    assert model.subject_cid == "bafyliked"


def test_parse_follow_happy(follow_event: dict[str, Any]) -> None:
    model = to_model(follow_event)
    assert isinstance(model, BskyFollow)
    assert model.subject_did == "did:plc:followed999"


def test_parse_post_without_reply(post_event: dict[str, Any]) -> None:
    """Edge: a top-level post has no reply block -> reply fields are None."""
    del post_event["commit"]["record"]["reply"]
    model = to_model(post_event)
    assert isinstance(model, BskyPost)
    assert model.reply_parent is None
    assert model.reply_root is None


def test_parse_missing_required_field_raises_keyerror(
    malformed_post_event: dict[str, Any],
) -> None:
    """Edge: a post record missing 'text' raises KeyError (the loop routes it to DLQ)."""
    with pytest.raises(KeyError):
        to_model(malformed_post_event)


def test_parse_delete_op_raises_valueerror(post_event: dict[str, Any]) -> None:
    """Edge: a delete op is not ingestable -> ValueError (the loop skips it quietly)."""
    post_event["commit"]["operation"] = "delete"
    with pytest.raises(ValueError, match="unsupported operation"):
        to_model(post_event)


def test_parse_non_commit_raises_valueerror(post_event: dict[str, Any]) -> None:
    post_event["kind"] = "identity"
    with pytest.raises(ValueError, match="not a commit"):
        to_model(post_event)


def test_parse_unknown_collection_raises_valueerror(post_event: dict[str, Any]) -> None:
    post_event["commit"]["collection"] = "app.bsky.graph.block"
    with pytest.raises(ValueError, match="unknown collection"):
        to_model(post_event)
