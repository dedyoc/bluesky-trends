"""Pure batch accumulator for the Kafka -> Iceberg bronze likes write.

Mirror of ``batch.py`` for the like event type: holds decoded LikeRows until a flush
threshold is hit (>=N rows OR elapsed >=T seconds), then materializes them as a single
pyarrow Table (one Iceberg append per flush). Tracks the max offset seen per partition so the
caller can commit offsets ONLY after the append succeeds (write-before-commit). No I/O.
"""

from __future__ import annotations

import pyarrow as pa

from bsky_dagster.transforms.bronze_schema_likes import likes_arrow_schema
from bsky_dagster.transforms.decode_likes import LikeRow


class LikesBatch:
    """Accumulates rows and the per-partition high-water offsets for one flush."""

    def __init__(self, max_rows: int, max_seconds: float) -> None:
        self._max_rows = max_rows
        self._max_seconds = max_seconds
        self._rows: list[LikeRow] = []
        # partition -> highest offset accumulated (for commit-after-write).
        self._offsets: dict[int, int] = {}

    def add(self, row: LikeRow) -> None:
        self._rows.append(row)
        prev = self._offsets.get(row.kafka_partition, -1)
        if row.kafka_offset > prev:
            self._offsets[row.kafka_partition] = row.kafka_offset

    def __len__(self) -> int:
        return len(self._rows)

    def is_empty(self) -> bool:
        return not self._rows

    def should_flush(self, *, elapsed_seconds: float) -> bool:
        """Flush when the row-count OR the time bound is reached (and we have rows)."""
        if self.is_empty():
            return False
        return len(self._rows) >= self._max_rows or elapsed_seconds >= self._max_seconds

    def offsets(self) -> dict[int, int]:
        """Per-partition highest offset in this batch (copy)."""
        return dict(self._offsets)

    def to_arrow(self) -> pa.Table:
        """Materialize the held rows as a pyarrow Table matching the bronze likes schema."""
        schema = likes_arrow_schema()
        columns: dict[str, list[object]] = {name: [] for name in schema.names}
        for r in self._rows:
            columns["did"].append(r.did)
            columns["rkey"].append(r.rkey)
            columns["cid"].append(r.cid)
            columns["created_at"].append(r.created_at)
            columns["subject_uri"].append(r.subject_uri)
            columns["subject_cid"].append(r.subject_cid)
            columns["kafka_partition"].append(r.kafka_partition)
            columns["kafka_offset"].append(r.kafka_offset)
            columns["ingest_ts"].append(r.ingest_ts)
        return pa.table(columns, schema=schema)

    def clear(self) -> None:
        self._rows.clear()
        self._offsets.clear()
