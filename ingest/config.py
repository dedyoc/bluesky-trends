"""Runtime configuration, read from the environment.

Defaults target the in-cluster systems (homelab-ops); docker-compose.dev.yml overrides
them via env for local runs.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGEST_", env_file=".env", extra="ignore")

    # Kafka / Schema Registry
    kafka_bootstrap: str = "bsky-kafka-bootstrap.kafka:9092"
    schema_registry_url: str = "http://bsky-kafka-bootstrap.kafka:8081"

    # Postgres cursor store
    postgres_dsn: str = "postgresql://bsky:bsky@localhost:5432/bsky_ingest"

    # Jetstream source (us-east primary per v1 decision)
    jetstream_url: str = "wss://jetstream2.us-east.bsky.network/subscribe"
    stream_name: str = "jetstream-main"

    # Cursor checkpoint: persist after this many acks OR this many seconds, whichever first.
    cursor_checkpoint_acks: int = 100
    cursor_checkpoint_seconds: float = 2.0

    # Reconnect backoff (exponential with jitter)
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    backoff_jitter_ratio: float = 0.2

    # How often to emit the stats log line.
    stats_interval_seconds: float = 10.0

    # Topic names
    topic_posts: str = "bsky.posts.v1"
    topic_likes: str = "bsky.likes.v1"
    topic_follows: str = "bsky.follows.v1"
    topic_dlq: str = "bsky.dlq.v1"

    wanted_collections: tuple[str, ...] = Field(
        default=(
            "app.bsky.feed.post",
            "app.bsky.feed.like",
            "app.bsky.graph.follow",
        )
    )
