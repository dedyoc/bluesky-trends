"""Wrap the dbt project (staging + marts) as Dagster assets.

@dbt_assets surfaces each dbt model as a Dagster asset and each dbt test (not_null, the
singular uniqueness tests) as a Dagster asset check automatically. The dbt models depend on
``posts_landing`` (the source table it loads) via the manifest's source mapping below.

The manifest is generated at build time (`dbt parse`) by the Makefile/`make dagster`; if it
is missing we skip wiring the dbt assets so the rest of the code graph still imports.
"""

import pathlib
from collections.abc import Iterator, Mapping
from typing import Any

from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from dagster import AssetExecutionContext

DBT_PROJECT_DIR = pathlib.Path(__file__).resolve().parents[3] / "dbt"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"


class _Translator(DagsterDbtTranslator):
    """Map the dbt source ``bsky.posts_bronze_raw`` onto the upstream ``posts_landing`` asset
    so Dagster knows landing -> staging -> mart ordering."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> Any:
        from dagster import AssetKey

        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(["posts_landing"])
        return super().get_asset_key(dbt_resource_props)


def build_dbt_assets() -> list[Any]:
    """Return the dbt asset defs if the manifest exists, else an empty list."""
    if not DBT_MANIFEST.exists():
        return []

    @dbt_assets(manifest=DBT_MANIFEST, dagster_dbt_translator=_Translator())
    def bsky_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
        yield from dbt.cli(["build"], context=context).stream()

    return [bsky_dbt_models]


def dbt_resource() -> DbtCliResource:
    return DbtCliResource(project_dir=str(DBT_PROJECT_DIR))
