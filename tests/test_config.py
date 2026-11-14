"""Tests for the ingest env-var contract (see README "Deployment contract")."""

from __future__ import annotations

import pathlib

import pytest
from pydantic import ValidationError

from ingest.config import Settings, load_settings

_REQUIRED = ("INGEST_KAFKA_BOOTSTRAP", "INGEST_SCHEMA_REGISTRY_URL", "INGEST_POSTGRES_DSN")


def test_endpoints_read_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_KAFKA_BOOTSTRAP", "k:9092")
    monkeypatch.setenv("INGEST_SCHEMA_REGISTRY_URL", "http://sr:8081")
    monkeypatch.setenv("INGEST_POSTGRES_DSN", "postgresql://u@pg:5432/db")
    monkeypatch.setenv("INGEST_JETSTREAM_URL", "wss://js.example/subscribe")
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert s.kafka_bootstrap == "k:9092"
    assert s.schema_registry_url == "http://sr:8081"
    assert s.postgres_dsn == "postgresql://u@pg:5432/db"
    assert s.jetstream_url == "wss://js.example/subscribe"


def test_missing_required_endpoints_fail_loudly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    for var in _REQUIRED:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)  # no .env in cwd -> load_settings sees only the real env
    with pytest.raises(ValidationError) as exc:
        load_settings()
    missing = {str(e["loc"][0]) for e in exc.value.errors()}
    assert missing == {"kafka_bootstrap", "schema_registry_url", "postgres_dsn"}
