"""ClickHouse access for the landing load + asset-check queries.

clickhouse-connect over the host HTTP port. Inserts are BATCHED (never row-by-row): the
landing load hands whole column blocks to ``insert``; with a block per call this is the
batched path the standards require. Read helpers back the asset checks.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from bsky_dagster.config import Settings


def client(settings: Settings) -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


def insert_block(
    ch: Client,
    table: str,
    rows: Sequence[Sequence[Any]],
    column_names: Sequence[str],
) -> int:
    """Insert one batched block. Returns the row count inserted (0 for an empty block)."""
    if not rows:
        return 0
    ch.insert(table, rows, column_names=list(column_names))
    return len(rows)


def scalar(ch: Client, query: str) -> Any:
    """Run a query expected to return a single value (asset-check helper)."""
    result = ch.query(query)
    if not result.result_rows:
        return None
    return result.result_rows[0][0]
