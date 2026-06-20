"""Dagster asset checks: freshness, volume, null-rate.

These are the platform-health gates the standards require. They are EVENT-time / data-shape
checks (not materialization-time), so they query the data directly rather than using the
build_*_freshness_checks helpers. dbt's not_null/unique tests provide additional per-column
asset checks automatically via @dbt_assets.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime

from bsky_dagster.config import Settings
from bsky_dagster.resources.clickhouse import client, scalar
from bsky_dagster.resources.iceberg import load_catalog
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

# Thresholds (kept here, not magic numbers in the queries).
_BRONZE_FRESHNESS_MAX_LAG_S = 24 * 3600  # bronze should hold an event from the last day
_MART_VOLUME_MIN_FRACTION = 0.25  # latest day must be >= 25% of the trailing avg


def _bronze_freshness(bronze_table: str) -> AssetCheckResult:
    """Shared body: newest archived event in ``bronze_table`` within the freshness window."""
    settings = Settings()
    table = load_catalog(settings).load_table(bronze_table)
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


def _mart_volume(mart: str) -> AssetCheckResult:
    """Shared body: latest day's row count in ``mart`` vs the trailing average (anomaly guard)."""
    settings = Settings()
    ch = client(settings)
    latest = scalar(
        ch,
        f"SELECT count() FROM {mart} FINAL WHERE day = (SELECT max(day) FROM {mart} FINAL)",
    )
    trailing = scalar(
        ch,
        f"SELECT avg(c) FROM (SELECT day, count() AS c FROM {mart} FINAL "
        f"WHERE day < (SELECT max(day) FROM {mart} FINAL) GROUP BY day)",
    )
    # With only one day of data the trailing window is empty: ClickHouse avg() over an empty
    # set returns NaN (not NULL/0), and `not NaN` is False — so guard NaN explicitly alongside
    # the no-rows case. No baseline -> nothing to compare -> pass.
    if not trailing or math.isnan(float(trailing)):
        return AssetCheckResult(
            passed=True, metadata={"latest_rows": latest or 0, "trailing_avg": "n/a"}
        )
    passed = (latest or 0) >= _MART_VOLUME_MIN_FRACTION * trailing
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"latest_rows": latest or 0, "trailing_avg": round(float(trailing), 1)},
    )


def _stg_null_rate(stg: str) -> AssetCheckResult:
    """Shared body: key columns must never be null/empty in ``stg`` (blocking)."""
    settings = Settings()
    ch = client(settings)
    bad = scalar(ch, f"SELECT countIf(did = '' OR rkey = '' OR cid = '') FROM {stg} FINAL")
    return AssetCheckResult(
        passed=(bad or 0) == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"empty_key_rows": bad or 0},
    )


@asset_check(asset=AssetKey(["posts_bronze"]), blocking=False)
def check_bronze_freshness() -> AssetCheckResult:
    """The newest archived event is within the freshness window (staleness alert, not error)."""
    return _bronze_freshness(Settings().bronze_table)


@asset_check(asset=AssetKey(["mart_posts_by_lang_daily"]), blocking=False)
def check_mart_volume() -> AssetCheckResult:
    """Latest day's row count is within a sane band of the trailing average (anomaly guard)."""
    return _mart_volume("mart_posts_by_lang_daily")


@asset_check(asset=AssetKey(["stg_posts"]), blocking=True)
def check_stg_null_rate() -> AssetCheckResult:
    """Key columns must never be null in staging (blocking — a null key breaks downstream)."""
    return _stg_null_rate("stg_posts")


@asset_check(asset=AssetKey(["likes_bronze"]), blocking=False)
def check_likes_bronze_freshness() -> AssetCheckResult:
    """The newest archived like is within the freshness window (staleness alert, not error)."""
    return _bronze_freshness(Settings().bronze_table_likes)


@asset_check(asset=AssetKey(["mart_likes_by_subject_daily"]), blocking=False)
def check_likes_mart_volume() -> AssetCheckResult:
    """Latest day's like-row count is within a sane band of the trailing average."""
    return _mart_volume("mart_likes_by_subject_daily")


@asset_check(asset=AssetKey(["stg_likes"]), blocking=True)
def check_likes_stg_null_rate() -> AssetCheckResult:
    """Key columns must never be null in likes staging (blocking)."""
    return _stg_null_rate("stg_likes")


@asset_check(asset=AssetKey(["follows_bronze"]), blocking=False)
def check_follows_bronze_freshness() -> AssetCheckResult:
    """The newest archived follow is within the freshness window (staleness alert, not error)."""
    return _bronze_freshness(Settings().bronze_table_follows)


@asset_check(asset=AssetKey(["mart_follows_by_subject_daily"]), blocking=False)
def check_follows_mart_volume() -> AssetCheckResult:
    """Latest day's follow-row count is within a sane band of the trailing average."""
    return _mart_volume("mart_follows_by_subject_daily")


@asset_check(asset=AssetKey(["stg_follows"]), blocking=True)
def check_follows_stg_null_rate() -> AssetCheckResult:
    """Key columns must never be null in follows staging (blocking)."""
    return _stg_null_rate("stg_follows")
