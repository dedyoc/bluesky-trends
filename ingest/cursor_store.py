"""Postgres-backed Jetstream cursor store for zero-gap resume.

The cursor is the Jetstream ``time_us`` of the last event whose Kafka produce was acked.
It is persisted AFTER produce-ack only (never before) — saving early is the classic
silent-data-gap bug: a crash between save and produce drops events. On resume we tolerate
a small replay window (sinks dedupe on (did, rkey/cid)).
"""

from __future__ import annotations

import asyncpg

_UPSERT = """
INSERT INTO ingest_cursors (stream_name, cursor, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (stream_name)
DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()
"""

_SELECT = "SELECT cursor FROM ingest_cursors WHERE stream_name = $1"


class CursorStore:
    """Loads and persists the per-stream cursor. Wraps an asyncpg pool."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> CursorStore:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        assert pool is not None  # create_pool returns None only if used as async ctx mgr
        return cls(pool)

    async def load(self, stream_name: str) -> int | None:
        """Return the saved cursor, or None for a fresh stream."""
        row = await self._pool.fetchrow(_SELECT, stream_name)
        return None if row is None else int(row["cursor"])

    async def save(self, stream_name: str, cursor: int) -> None:
        """UPSERT the cursor. Call ONLY after the corresponding produce was acked."""
        await self._pool.execute(_UPSERT, stream_name, cursor)

    async def close(self) -> None:
        await self._pool.close()
