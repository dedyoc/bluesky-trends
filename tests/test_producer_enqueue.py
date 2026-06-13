"""Tests for the producer's BufferError backpressure handling.

Exercises IngestProducer._enqueue in isolation (no Schema Registry, no broker) by binding
the method to a stub object that only has a fake librdkafka producer. This proves that a
full local queue triggers poll-to-drain-and-retry rather than dropping the message.
"""

from __future__ import annotations

from typing import Any

import pytest

from ingest.producer import IngestProducer


class FakeRdkProducer:
    """Raises BufferError on the first ``fail_times`` produce() calls, then succeeds."""

    def __init__(self, fail_times: int) -> None:
        self._fail_times = fail_times
        self.produce_attempts = 0
        self.polls = 0
        self.delivered: list[Any] = []

    def produce(self, **kwargs: Any) -> None:
        self.produce_attempts += 1
        if self.produce_attempts <= self._fail_times:
            raise BufferError("queue full")
        self.delivered.append(kwargs)

    def poll(self, timeout: float) -> None:
        self.polls += 1


def _stub_producer(rdk: FakeRdkProducer) -> IngestProducer:
    # _enqueue() reads only self._producer (not _settings/_serializers), so bypassing
    # __init__ here is safe — it avoids needing a Schema Registry / broker for this unit.
    stub = IngestProducer.__new__(IngestProducer)
    stub._producer = rdk  # type: ignore[assignment]
    return stub


def _noop(err: Any, msg: Any) -> None:
    pass


def test_enqueue_succeeds_first_try() -> None:
    rdk = FakeRdkProducer(fail_times=0)
    _stub_producer(rdk)._enqueue("t", b"v", key=b"k", on_delivery=_noop)
    assert rdk.produce_attempts == 1
    assert rdk.polls == 0
    assert len(rdk.delivered) == 1


def test_enqueue_retries_on_buffererror_then_succeeds() -> None:
    rdk = FakeRdkProducer(fail_times=3)
    _stub_producer(rdk)._enqueue("t", b"v", key=b"k", on_delivery=_noop)
    # 3 failures (each followed by a poll-to-drain) + 1 success.
    assert rdk.produce_attempts == 4
    assert rdk.polls == 3
    assert len(rdk.delivered) == 1


def test_enqueue_propagates_if_queue_never_drains() -> None:
    # Fails on every attempt: 5 retries + 1 final attempt that re-raises -> never dropped.
    rdk = FakeRdkProducer(fail_times=99)
    with pytest.raises(BufferError):
        _stub_producer(rdk)._enqueue("t", b"v", key=b"k", on_delivery=_noop)
    assert rdk.delivered == []
