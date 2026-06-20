"""The Dagster code location: assets + asset checks + the dbt resource.

Loaded by ``dagster dev`` (see `make dagster`, which runs with the repo's ``dagster/`` dir on
the path so ``bsky_dagster`` imports cleanly). Per event type the graph is:

    <type>_bronze (Kafka -> Iceberg)
      -> <type>_landing (Iceberg -> ClickHouse)
      -> dbt models (stg_<type> -> mart_<type>_...)

for posts (by-lang), likes (by-subject), and follows (by-subject). dbt models + their
not_null/unique tests come from @dbt_assets (wired only when the dbt manifest exists); the
explicit checks add freshness/volume/null-rate gates per event type.
"""

from __future__ import annotations

from bsky_dagster.assets.bronze import posts_bronze
from bsky_dagster.assets.bronze_follows import follows_bronze
from bsky_dagster.assets.bronze_likes import likes_bronze
from bsky_dagster.assets.dbt import build_dbt_assets, dbt_resource
from bsky_dagster.assets.landing import posts_landing
from bsky_dagster.assets.landing_follows import follows_landing
from bsky_dagster.assets.landing_likes import likes_landing
from bsky_dagster.checks import (
    check_bronze_freshness,
    check_follows_bronze_freshness,
    check_follows_mart_volume,
    check_follows_stg_null_rate,
    check_likes_bronze_freshness,
    check_likes_mart_volume,
    check_likes_stg_null_rate,
    check_mart_volume,
    check_stg_null_rate,
)
from dagster import Definitions

defs = Definitions(
    assets=[
        posts_bronze,
        posts_landing,
        likes_bronze,
        likes_landing,
        follows_bronze,
        follows_landing,
        *build_dbt_assets(),
    ],
    asset_checks=[
        check_bronze_freshness,
        check_mart_volume,
        check_stg_null_rate,
        check_likes_bronze_freshness,
        check_likes_mart_volume,
        check_likes_stg_null_rate,
        check_follows_bronze_freshness,
        check_follows_mart_volume,
        check_follows_stg_null_rate,
    ],
    resources={"dbt": dbt_resource()},
)
