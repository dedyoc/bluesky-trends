"""Iceberg REST catalog access (PyIceberg over the tabulario/iceberg-rest dev catalog).

The connection props below were validated against the local REST catalog + MinIO: a table
create/append/scan round-trip succeeds with path-style S3 access on the host-published
ports. Prod overrides the URIs/creds via env (see config.Settings).
"""

from __future__ import annotations

from typing import cast

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table

from bsky_dagster.config import Settings


def load_catalog(settings: Settings) -> RestCatalog:
    """Build a RestCatalog client from settings. S3FileIO talks to MinIO path-style."""
    return RestCatalog(
        name="bsky",
        **{
            "uri": settings.iceberg_rest_uri,
            "warehouse": settings.iceberg_warehouse,
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.s3_access_key_id,
            "s3.secret-access-key": settings.s3_secret_access_key,
            "s3.path-style-access": "true",
            "s3.region": settings.s3_region,
        },
    )


def ensure_table(
    catalog: RestCatalog,
    settings: Settings,
    *,
    table_name: str,
    schema: Schema,
    spec: PartitionSpec,
) -> Table:
    """Create the bronze namespace+``table_name`` (day-partitioned) if absent, then return it.

    The schema and partition spec are passed in by the caller so one helper serves every
    event type's bronze table (posts/likes/follows)."""
    catalog.create_namespace_if_not_exists(settings.iceberg_namespace)
    if not catalog.table_exists(table_name):
        catalog.create_table(table_name, schema=schema, partition_spec=spec)
    return cast(Table, catalog.load_table(table_name))
