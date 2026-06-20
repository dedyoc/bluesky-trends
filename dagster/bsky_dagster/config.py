"""Runtime configuration for the v2 batch layer, read from the environment.

Mirrors ``ingest/config.py``'s pydantic-settings pattern but with its own env prefix
(``BSKY_DAGSTER_``). Defaults target the local docker-compose stack via the HOST-published
ports (Dagster runs on the host, `make dagster`, not inside the compose network), so the
Kafka host listener (localhost:19092), the host ClickHouse (localhost:8123), the host
Iceberg REST catalog (localhost:8181) and the host MinIO S3 endpoint (localhost:9100) are
the defaults. Prod overrides these via env to the in-cluster addresses.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BSKY_DAGSTER_", env_file=".env", extra="ignore")

    # Kafka source (host listener; prod: bsky-kafka-bootstrap.kafka:9092)
    kafka_bootstrap: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:8081"
    topic_posts: str = "bsky.posts.v1"
    topic_likes: str = "bsky.likes.v1"
    topic_follows: str = "bsky.follows.v1"
    topic_dlq: str = "bsky.dlq.v1"
    # Dedicated consumer group per event type for the bronze archiver; offsets are the resume
    # cursor (one group per topic so likes/follows archive independently of posts).
    bronze_consumer_group: str = "dagster-bronze-posts"
    bronze_consumer_group_likes: str = "dagster-bronze-likes"
    bronze_consumer_group_follows: str = "dagster-bronze-follows"

    # Batch flush thresholds for the Kafka -> Iceberg consume (batched, never row-by-row):
    # flush when EITHER the row count or the idle-time bound is reached.
    bronze_batch_max_rows: int = 10_000
    bronze_batch_max_seconds: float = 5.0
    # Stop a single materialization once the topic is drained (partition EOF) or this many
    # rows have been archived — keeps an asset run bounded.
    bronze_max_rows_per_run: int = 500_000

    # Iceberg REST catalog + MinIO (S3). Host-published ports (S3 API remapped to 9100).
    iceberg_rest_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3://iceberg/"
    s3_endpoint: str = "http://localhost:9100"
    s3_access_key_id: str = "minio"
    s3_secret_access_key: str = "minio12345"
    s3_region: str = "us-east-1"
    iceberg_namespace: str = "bronze"
    bronze_table: str = "bronze.posts"
    bronze_table_likes: str = "bronze.likes"
    bronze_table_follows: str = "bronze.follows"

    # ClickHouse (host HTTP port; default user, empty password in dev).
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "bsky"
    # Landing table the dbt staging model reads from (one per event type).
    landing_table: str = "posts_bronze_raw"
    landing_table_likes: str = "likes_bronze_raw"
    landing_table_follows: str = "follows_bronze_raw"
    # Async-batched insert size for the Iceberg -> ClickHouse landing load (never row-by-row).
    landing_batch_rows: int = 50_000
