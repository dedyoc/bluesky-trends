"""Tests for the pure BronzeBatch accumulator (flush thresholds + offset tracking)."""

from __future__ import annotations

from datetime import UTC, datetime

from bsky_dagster.transforms.batch import BronzeBatch
from bsky_dagster.transforms.decode import BronzeRow

_TS = datetime(2026, 6, 18, tzinfo=UTC)


def _row(partition: int, offset: int) -> BronzeRow:
    return BronzeRow(
        did="did:plc:x",
        rkey=f"r{offset}",
        cid="c",
        created_at=_TS,
        text="t",
        langs=[],
        reply_parent=None,
        reply_root=None,
        kafka_partition=partition,
        kafka_offset=offset,
        ingest_ts=_TS,
    )


def test_empty_batch_never_flushes() -> None:
    batch = BronzeBatch(max_rows=10, max_seconds=5.0)
    assert batch.is_empty()
    assert batch.should_flush(elapsed_seconds=999.0) is False


def test_flush_on_row_count() -> None:
    batch = BronzeBatch(max_rows=3, max_seconds=999.0)
    batch.add(_row(0, 0))
    batch.add(_row(0, 1))
    assert batch.should_flush(elapsed_seconds=0.0) is False
    batch.add(_row(0, 2))
    assert batch.should_flush(elapsed_seconds=0.0) is True


def test_flush_on_time_bound() -> None:
    batch = BronzeBatch(max_rows=10_000, max_seconds=5.0)
    batch.add(_row(0, 0))
    assert batch.should_flush(elapsed_seconds=4.9) is False
    assert batch.should_flush(elapsed_seconds=5.0) is True


def test_offsets_track_per_partition_high_water() -> None:
    batch = BronzeBatch(max_rows=100, max_seconds=5.0)
    batch.add(_row(0, 5))
    batch.add(_row(1, 2))
    batch.add(_row(0, 9))  # higher on partition 0
    batch.add(_row(1, 1))  # lower on partition 1 -> ignored
    assert batch.offsets() == {0: 9, 1: 2}


def test_to_arrow_shapes_rows() -> None:
    batch = BronzeBatch(max_rows=100, max_seconds=5.0)
    batch.add(_row(0, 0))
    batch.add(_row(0, 1))
    table = batch.to_arrow()
    assert table.num_rows == 2
    assert table.column("kafka_offset").to_pylist() == [0, 1]


def test_clear_resets_rows_and_offsets() -> None:
    batch = BronzeBatch(max_rows=100, max_seconds=5.0)
    batch.add(_row(0, 0))
    batch.clear()
    assert batch.is_empty()
    assert batch.offsets() == {}
