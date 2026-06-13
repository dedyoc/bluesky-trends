"""Tests for the pure backoff and URL-building helpers (no real websocket)."""

from __future__ import annotations

from ingest.config import Settings
from ingest.jetstream_client import backoff_delay, build_url


def _settings() -> Settings:
    return Settings(
        backoff_initial_seconds=1.0,
        backoff_max_seconds=60.0,
        backoff_jitter_ratio=0.2,
    )


def test_backoff_grows_exponentially_midpoint() -> None:
    s = _settings()
    # rand=0.5 -> zero jitter, so we see the bare exponential base.
    assert backoff_delay(0, s, rand=0.5) == 1.0
    assert backoff_delay(1, s, rand=0.5) == 2.0
    assert backoff_delay(2, s, rand=0.5) == 4.0


def test_backoff_capped_at_max() -> None:
    s = _settings()
    assert backoff_delay(100, s, rand=0.5) == 60.0


def test_backoff_jitter_bounds() -> None:
    s = _settings()
    # attempt 0 base=1.0, jitter +/-20% -> [0.8, 1.2]
    assert backoff_delay(0, s, rand=0.0) == 1.0 - 0.2
    assert backoff_delay(0, s, rand=1.0) == 1.0 + 0.2


def test_backoff_never_negative() -> None:
    s = Settings(backoff_initial_seconds=0.0, backoff_jitter_ratio=1.0)
    assert backoff_delay(0, s, rand=0.0) >= 0.0


def test_build_url_includes_wanted_collections_and_cursor() -> None:
    s = _settings()
    url = build_url(s, 123)
    assert url.startswith(s.jetstream_url + "?")
    assert "wantedCollections=app.bsky.feed.post" in url
    assert "wantedCollections=app.bsky.feed.like" in url
    assert "wantedCollections=app.bsky.graph.follow" in url
    assert "cursor=123" in url


def test_build_url_omits_cursor_when_none() -> None:
    url = build_url(_settings(), None)
    assert "cursor=" not in url
