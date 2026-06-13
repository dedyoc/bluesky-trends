"""Integration tests for the ingest loop's routing decisions.

Verifies that _consume() routes each event class correctly: valid -> produce, malformed
(KeyError/ValidationError) -> DLQ, non-ingestable (ValueError) -> skipped, and a parseable
event with no usable time_us -> DLQ. These protect the exception-routing that the parse
tests alone can't (parse tests only prove to_model raises the right exception).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from ingest.config import Settings
from ingest.main import _consume, _CursorCheckpointer
from ingest.metrics import Metrics
from ingest.producer import DeliveryCallback
from schemas.models import BskyFollow, BskyLike, BskyPost


class FakeStore:
    def __init__(self) -> None:
        self.saved: list[int] = []

    async def save(self, stream_name: str, cursor: int) -> None:
        self.saved.append(cursor)


class FakeProducer:
    def __init__(self) -> None:
        self.produced: list[tuple[BskyPost | BskyLike | BskyFollow, int]] = []
        self.dlq: list[tuple[str, str, str]] = []
        self._pending: list[tuple[DeliveryCallback, int]] = []

    def produce(
        self,
        model: BskyPost | BskyLike | BskyFollow,
        *,
        cursor: int,
        on_delivery: DeliveryCallback,
    ) -> None:
        self.produced.append((model, cursor))
        self._pending.append((on_delivery, cursor))

    def produce_dlq(self, raw_payload: str, error: str, intended_topic: str) -> None:
        self.dlq.append((raw_payload, error, intended_topic))

    def poll(self, timeout: float = 0.0) -> None:
        # Simulate broker acks arriving when the loop polls.
        for cb, cursor in self._pending:
            cb(True, cursor)
        self._pending.clear()

    def flush(self, timeout: float = 30.0) -> int:
        self.poll(0)
        return 0


class FakeClient:
    def __init__(self, events: list[dict[str, Any]]) -> None:
        self._events = events

    async def events(self, cursor: int | None) -> AsyncIterator[dict[str, Any]]:
        for ev in self._events:
            yield ev


def _settings() -> Settings:
    return Settings(cursor_checkpoint_acks=1, cursor_checkpoint_seconds=0.0)


async def _drive(events: list[dict[str, Any]]) -> tuple[FakeProducer, FakeStore]:
    settings = _settings()
    producer = FakeProducer()
    store = FakeStore()
    checkpointer = _CursorCheckpointer(store, settings)  # type: ignore[arg-type]
    await _consume(
        settings,
        asyncio.Event(),
        client=FakeClient(events),
        producer=producer,
        checkpointer=checkpointer,
        metrics=Metrics(),
        start_cursor=None,
    )
    return producer, store


async def test_valid_post_is_produced(post_event: dict[str, Any]) -> None:
    producer, store = await _drive([post_event])
    assert len(producer.produced) == 1
    assert producer.dlq == []
    model, cursor = producer.produced[0]
    assert isinstance(model, BskyPost)
    assert cursor == post_event["time_us"]
    # ack arrived via poll -> watermark saved
    assert store.saved == [post_event["time_us"]]


async def test_malformed_post_is_routed_to_dlq(malformed_post_event: dict[str, Any]) -> None:
    producer, _ = await _drive([malformed_post_event])
    assert producer.produced == []
    assert len(producer.dlq) == 1
    _, error, intended_topic = producer.dlq[0]
    assert "KeyError" in error
    assert intended_topic == "app.bsky.feed.post"


async def test_delete_op_is_skipped_not_dlq(post_event: dict[str, Any]) -> None:
    post_event["commit"]["operation"] = "delete"
    producer, _ = await _drive([post_event])
    assert producer.produced == []
    assert producer.dlq == []  # non-ingestable shape is skipped quietly


async def test_missing_time_us_is_routed_to_dlq(post_event: dict[str, Any]) -> None:
    del post_event["time_us"]
    producer, _ = await _drive([post_event])
    assert producer.produced == []
    assert len(producer.dlq) == 1
    assert producer.dlq[0][1] == "missing or non-int time_us"


async def test_mixed_stream_routes_each_correctly(
    post_event: dict[str, Any],
    like_event: dict[str, Any],
    follow_event: dict[str, Any],
    malformed_post_event: dict[str, Any],
) -> None:
    producer, _ = await _drive([post_event, malformed_post_event, like_event, follow_event])
    assert len(producer.produced) == 3  # post, like, follow
    assert len(producer.dlq) == 1  # malformed post
