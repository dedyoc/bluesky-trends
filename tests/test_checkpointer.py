"""Tests for the gap-free cursor watermark — the core data-safety logic.

These cover the exact defect that motivated the contiguous-prefix design: an out-of-order
or failed delivery must never let the saved cursor advance past a gap.
"""

from __future__ import annotations

import pytest

from ingest.main import _CursorCheckpointer


class FakeStore:
    def __init__(self) -> None:
        self.saved: list[int] = []

    async def save(self, stream_name: str, cursor: int) -> None:
        self.saved.append(cursor)


class FakeSettings:
    cursor_checkpoint_acks = 1
    cursor_checkpoint_seconds = 0.0
    stream_name = "test-stream"


def _checkpointer() -> tuple[_CursorCheckpointer, FakeStore]:
    store = FakeStore()
    cp = _CursorCheckpointer(store, FakeSettings())  # type: ignore[arg-type]
    return cp, store


async def test_in_order_all_acked_advances_to_latest() -> None:
    cp, store = _checkpointer()
    for c in (10, 20, 30):
        cp.register(c)
    for c in (10, 20, 30):
        cp.on_ack(True, c)
    await cp.maybe_save(now=1.0)
    assert store.saved == [30]


async def test_earlier_failure_pins_watermark_below_gap() -> None:
    """Out-of-order: a later success must NOT persist a cursor past an earlier failure."""
    cp, store = _checkpointer()
    cp.register(1000)
    cp.register(2000)
    cp.on_ack(True, 2000)  # later event acks first
    cp.on_ack(False, 1000)  # earlier event FAILS
    await cp.maybe_save(now=1.0)
    assert store.saved == []  # nothing saved — gap at 1000 pins the watermark


async def test_out_of_order_success_then_gap_fill() -> None:
    """Watermark advances only across the contiguous acked prefix, folding in on gap-fill."""
    cp, store = _checkpointer()
    for c in (1000, 2000, 3000):
        cp.register(c)
    cp.on_ack(True, 3000)  # 3000 acks first; 1000/2000 still pending
    cp.on_ack(True, 1000)  # 1000 acks -> watermark can reach 1000 only
    await cp.maybe_save(now=1.0)
    assert store.saved == [1000]
    cp.on_ack(True, 2000)  # gap fills -> 2000 and 3000 fold in contiguously
    await cp.maybe_save(now=2.0)
    assert store.saved == [1000, 3000]


async def test_no_redundant_save_when_watermark_unchanged() -> None:
    cp, store = _checkpointer()
    cp.register(10)
    cp.on_ack(True, 10)
    await cp.maybe_save(now=1.0)
    await cp.maybe_save(now=2.0)  # nothing new acked
    assert store.saved == [10]


async def test_checkpoint_cadence_by_ack_count() -> None:
    store = FakeStore()

    class S:
        cursor_checkpoint_acks = 3
        cursor_checkpoint_seconds = 9999.0  # time cadence effectively disabled
        stream_name = "s"

    cp = _CursorCheckpointer(store, S())  # type: ignore[arg-type]
    for c in (1, 2):
        cp.register(c)
        cp.on_ack(True, c)
    await cp.maybe_save(now=0.0)
    assert store.saved == []  # only 2 acks, threshold is 3
    cp.register(3)
    cp.on_ack(True, 3)
    await cp.maybe_save(now=0.0)
    assert store.saved == [3]  # 3rd ack trips the count threshold


async def test_final_save_flushes_unsaved_watermark() -> None:
    store = FakeStore()

    class S:
        cursor_checkpoint_acks = 9999  # never trips by count
        cursor_checkpoint_seconds = 9999.0  # never trips by time
        stream_name = "s"

    cp = _CursorCheckpointer(store, S())  # type: ignore[arg-type]
    cp.register(42)
    cp.on_ack(True, 42)
    await cp.maybe_save(now=0.0)
    assert store.saved == []  # cadence never tripped
    await cp.final_save()
    assert store.saved == [42]  # drain on shutdown persists it


@pytest.mark.parametrize("bad_cursor", [500])
async def test_ack_for_unregistered_cursor_is_ignored(bad_cursor: int) -> None:
    cp, store = _checkpointer()
    cp.on_ack(True, bad_cursor)  # never registered
    await cp.maybe_save(now=1.0)
    assert store.saved == []
