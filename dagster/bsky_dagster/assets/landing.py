"""Iceberg bronze -> ClickHouse landing asset.

Crosses the Iceberg->ClickHouse boundary exactly once: reads bronze rows newer than the
landing table's high-water ``ingest_ts`` and batch-inserts them into ``posts_bronze_raw``
(append-only MergeTree). The dbt ``stg_posts`` model then dedupes from there. Inserts go in
large blocks (never row-by-row); the bronze scan is filtered by the watermark so re-runs are
incremental, not full reloads.

Depends on ``posts_bronze`` so Dagster materializes bronze first.
"""

from __future__ import annotations

import pyarrow as pa
import structlog
from clickhouse_connect.driver.client import Client

from bsky_dagster.config import Settings
from bsky_dagster.resources.clickhouse import client, insert_block, scalar
from bsky_dagster.resources.iceberg import load_catalog
from bsky_dagster.transforms.bronze_schema import BRONZE_COLUMNS
from dagster import MaterializeResult, asset

log = structlog.get_logger("bsky_dagster.landing")


@asset(
    name="posts_landing",
    deps=["posts_bronze"],
    description="Load new Iceberg bronze rows into the ClickHouse landing table (incremental).",
    group_name="landing",
)
def posts_landing() -> MaterializeResult:
    settings = Settings()
    ch = client(settings)
    catalog = load_catalog(settings)
    table = catalog.load_table(settings.bronze_table)

    # High-water mark already landed. ClickHouse max() over an EMPTY table returns the epoch
    # default (1970-01-01), not NULL, so gate on the row count: empty landing -> load all.
    landed_so_far = scalar(ch, f"SELECT count() FROM {settings.landing_table}")
    watermark = None
    if landed_so_far:
        watermark = scalar(ch, f"SELECT max(ingest_ts) FROM {settings.landing_table}")

    scan = table.scan()
    if watermark is not None:
        # Iceberg row filter on ingest_ts; PyIceberg's timestamptz literal needs a zone offset.
        ts = watermark.isoformat()
        if watermark.tzinfo is None:
            ts += "+00:00"
        scan = table.scan(row_filter=f"ingest_ts > '{ts}'")

    arrow = scan.to_arrow()
    loaded = 0
    if arrow.num_rows:
        loaded = _insert_arrow(ch, settings.landing_table, arrow, settings.landing_batch_rows)

    total = scalar(ch, f"SELECT count() FROM {settings.landing_table}")
    log.info("posts_landing_done", loaded=loaded, landing_total=total, watermark=str(watermark))
    return MaterializeResult(metadata={"loaded_this_run": loaded, "landing_total_rows": total})


def _insert_arrow(ch: Client, table: str, arrow: pa.Table, batch_rows: int) -> int:
    """Insert a pyarrow Table into ClickHouse in row-blocks of ``batch_rows`` (never 1-by-1)."""
    # Project to the bronze column order the landing table expects.
    cols = list(BRONZE_COLUMNS)
    arrow = arrow.select(cols)
    total = 0
    for start in range(0, arrow.num_rows, batch_rows):
        chunk = arrow.slice(start, batch_rows)
        rows = [
            tuple(vals) for vals in zip(*[chunk.column(c).to_pylist() for c in cols], strict=True)
        ]
        total += insert_block(ch, table, rows, cols)
    return total
