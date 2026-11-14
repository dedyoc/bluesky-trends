"""Test helper: Settings with dummy values for the required endpoint fields."""

from __future__ import annotations

from typing import Any

from ingest.config import Settings

TEST_ENDPOINTS: dict[str, str] = {
    "kafka_bootstrap": "kafka.test:9092",
    "schema_registry_url": "http://registry.test:8081",
    "postgres_dsn": "postgresql://test@db.test:5432/test",
}


def make_settings(**overrides: Any) -> Settings:
    """Settings with dummy required endpoints; never reads a local .env file."""
    # _env_file is a pydantic-settings init kwarg mypy (no pydantic plugin) does not know.
    return Settings(_env_file=None, **{**TEST_ENDPOINTS, **overrides})  # type: ignore[call-arg]
