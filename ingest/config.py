"""Runtime configuration, read from the environment (prefix ``INGEST_``).

DEPLOYMENT CONTRACT: every field here is an env var the ingest image reads, documented in
README.md "Deployment contract" — bluesky-trends-deploy builds its manifests from that
section. Any change to a field (add/rename/default) MUST update the README in the same commit.

The three infrastructure endpoints (Kafka bootstrap, schema registry, Postgres DSN) are
REQUIRED with no default: a missing var fails loudly at startup instead of silently dialing a
wrong host. docker-compose.dev.yml sets them for local runs; k8s sets them in the deploy repo.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGEST_", env_file=".env", extra="ignore")

    # Kafka / Schema Registry (required)
    kafka_bootstrap: str
    schema_registry_url: str

    # Postgres cursor store (required; contains credentials -> a Secret in k8s)
    postgres_dsn: str

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


def load_settings() -> Settings:
    """Build Settings from the environment; raises ValidationError if a required var is unset."""
    # mypy sees the required fields as missing constructor args; pydantic-settings fills
    # them from INGEST_* env vars at runtime.
    return Settings()  # type: ignore[call-arg]
