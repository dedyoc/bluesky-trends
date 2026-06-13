"""Jetstream websocket consumer with reconnect (exponential backoff + jitter).

Connects to the Bluesky Jetstream firehose, requesting only the collections we ingest,
and resumes from a microsecond ``cursor`` if provided. Yields decoded JSON events. No
business logic here — parsing into typed models happens in ``ingest.parse``.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode

import structlog
import websockets

from ingest.config import Settings

log = structlog.get_logger("ingest.jetstream")


def backoff_delay(attempt: int, settings: Settings, *, rand: float) -> float:
    """Exponential backoff capped at backoff_max, with +/- jitter_ratio jitter.

    ``attempt`` starts at 0. ``rand`` is a [0,1) value (injected for testability);
    the returned delay is in seconds and never negative.
    """
    base: float = min(
        settings.backoff_initial_seconds * (2**attempt),
        settings.backoff_max_seconds,
    )
    jitter: float = base * settings.backoff_jitter_ratio * (2 * rand - 1)
    return max(0.0, base + jitter)


def build_url(settings: Settings, cursor: int | None) -> str:
    params: list[tuple[str, str]] = [("wantedCollections", c) for c in settings.wanted_collections]
    if cursor is not None:
        params.append(("cursor", str(cursor)))
    return f"{settings.jetstream_url}?{urlencode(params)}"


class JetstreamClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def events(self, cursor: int | None) -> AsyncIterator[dict[str, Any]]:
        """Yield decoded Jetstream events, reconnecting forever with backoff.

        ``cursor`` seeds the initial connection; callers should track the latest
        acked cursor themselves for the next process restart.
        """
        attempt = 0
        latest_cursor = cursor
        while True:
            try:
                url = build_url(self._settings, latest_cursor)
                async with websockets.connect(url, max_size=None) as ws:
                    log.info("jetstream_connected", cursor=latest_cursor)
                    attempt = 0  # reset backoff on a successful connection
                    async for raw in ws:
                        event = json.loads(raw)
                        # Track time_us so an intra-process reconnect resumes correctly.
                        ts = event.get("time_us")
                        if isinstance(ts, int):
                            latest_cursor = ts
                        yield event
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = backoff_delay(attempt, self._settings, rand=random.random())  # noqa: S311
                log.warning(
                    "jetstream_reconnect",
                    attempt=attempt,
                    delay_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                attempt += 1
