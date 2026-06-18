"""Dagster asset checks: freshness, volume, null-rate.

These are the platform-health gates the standards require. They are EVENT-time / data-shape
checks (not materialization-time), so they query the data directly rather than using the
build_*_freshness_checks helpers. dbt's not_null/unique tests provide additional per-column
asset checks automatically via @dbt_assets.
"""

from __future__ import annotations

from datetime import UTC, datetime

from bsky_dagster.config import Settings
from bsky_dagster.resources.clickhouse import client, scalar
from bsky_dagster.resources.iceberg import load_catalog
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

# Thresholds (kept here, not magic numbers in the queries).
_BRONZE_FRESHNESS_MAX_LAG_S = 24 * 3600  # bronze should hold an event from the last day
_MART_VOLUME_MIN_FRACTION = 0.25  # latest day must be >= 25% of the trailing avg


@asset_check(asset=AssetKey(["posts_bronze"]), blocking=False)
def check_bronze_freshness() -> AssetCheckResult:
    """The newest archived event is within the freshness window (staleness alert, not error)."""
    settings = Settings()
    table = load_catalog(settings).load_table(settings.bronze_table)
    df = table.scan(selected_fields=("created_at",)).to_arrow()
    if df.num_rows == 0:
        return AssetCheckResult(passed=False, metadata={"reason": "bronze empty"})
    newest = max(df.column("created_at").to_pylist())
    lag_s = (datetime.now(tz=UTC) - newest).total_seconds()
    return AssetCheckResult(
        passed=lag_s <= _BRONZE_FRESHNESS_MAX_LAG_S,
        severity=AssetCheckSeverity.WARN,
        metadata={"newest_event": str(newest), "lag_seconds": round(lag_s, 1)},
    )


@asset_check(asset=AssetKey(["mart_posts_by_lang_daily"]), blocking=False)
def check_mart_volume() -> AssetCheckResult:
    """Latest day's row count is within a sane band of the trailing average (anomaly guard)."""
    settings = Settings()
    ch = client(settings)
    latest = scalar(
        ch,
        "SELECT count() FROM mart_posts_by_lang_daily FINAL "
        "WHERE day = (SELECT max(day) FROM mart_posts_by_lang_daily FINAL)",
    )
    trailing = scalar(
        ch,
        "SELECT avg(c) FROM (SELECT day, count() AS c FROM mart_posts_by_lang_daily FINAL "
        "WHERE day < (SELECT max(day) FROM mart_posts_by_lang_daily FINAL) GROUP BY day)",
    )
    # With only one day of data there is no trailing baseline — pass (nothing to compare).
    if not trailing:
        return AssetCheckResult(
            passed=True, metadata={"latest_rows": latest or 0, "trailing_avg": "n/a"}
        )
    passed = (latest or 0) >= _MART_VOLUME_MIN_FRACTION * trailing
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"latest_rows": latest or 0, "trailing_avg": round(float(trailing), 1)},
    )


@asset_check(asset=AssetKey(["stg_posts"]), blocking=True)
def check_stg_null_rate() -> AssetCheckResult:
    """Key columns must never be null in staging (blocking — a null key breaks downstream)."""
    settings = Settings()
    ch = client(settings)
    bad = scalar(
        ch,
        "SELECT countIf(did = '' OR rkey = '' OR cid = '') FROM stg_posts FINAL",
    )
    return AssetCheckResult(
        passed=(bad or 0) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"empty_key_rows": bad or 0},
    )
