"""Tests for the Postgres cursor store. asyncpg is mocked — no real DB, no network."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

from ingest.cursor_store import CursorStore


def _store_with_pool() -> tuple[CursorStore, AsyncMock]:
    pool = AsyncMock()
    return CursorStore(pool), pool


async def test_load_returns_none_for_fresh_stream() -> None:
    store, pool = _store_with_pool()
    pool.fetchrow.return_value = None
    assert await store.load("jetstream-main") is None
    pool.fetchrow.assert_awaited_once()


async def test_load_returns_saved_cursor() -> None:
    store, pool = _store_with_pool()
    pool.fetchrow.return_value = {"cursor": 1700000000000000}
    assert await store.load("jetstream-main") == 1700000000000000


async def test_save_executes_upsert_with_args() -> None:
    store, pool = _store_with_pool()
    await store.save("jetstream-main", 12345)
    pool.execute.assert_awaited_once()
    args: tuple[Any, ...] = pool.execute.await_args.args
    sql = args[0]
    assert "INSERT INTO ingest_cursors" in sql
    assert "ON CONFLICT" in sql
    # updated_at must refresh on conflict (the reviewer-flagged correctness point).
    assert "updated_at = now()" in sql
    assert args[1] == "jetstream-main"
    assert args[2] == 12345


async def test_close_closes_pool() -> None:
    store, pool = _store_with_pool()
    await store.close()
    pool.close.assert_awaited_once()
