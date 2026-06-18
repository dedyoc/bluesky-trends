"""The Dagster code location: assets + asset checks + the dbt resource.

Loaded by ``dagster dev`` (see `make dagster`, which runs with the repo's ``dagster/`` dir on
the path so ``bsky_dagster`` imports cleanly). The graph is:

    posts_bronze (Kafka -> Iceberg)
      -> posts_landing (Iceberg -> ClickHouse)
      -> dbt models (stg_posts -> mart_posts_by_lang_daily)

dbt models + their not_null/unique tests come from @dbt_assets (wired only when the dbt
manifest exists); the three explicit checks add freshness/volume/null-rate gates.
"""

from __future__ import annotations

from bsky_dagster.assets.bronze import posts_bronze
from bsky_dagster.assets.dbt import build_dbt_assets, dbt_resource
from bsky_dagster.assets.landing import posts_landing
from bsky_dagster.checks import (
    check_bronze_freshness,
    check_mart_volume,
    check_stg_null_rate,
)
from dagster import Definitions

defs = Definitions(
    assets=[posts_bronze, posts_landing, *build_dbt_assets()],
    asset_checks=[check_bronze_freshness, check_mart_volume, check_stg_null_rate],
    resources={"dbt": dbt_resource()},
)
