"""Structured logging + lightweight in-process counters.

Every service exports last_event_ts + counters so staleness can be alerted on (a quiet
dashboard must be distinguishable from a dead pipeline). No print() anywhere.
"""

from __future__ import annotations

import logging
from collections import Counter

import structlog

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    cache_logger_on_first_use=True,
)


class Metrics:
    """Accumulates event/DLQ counts and the last event timestamp for staleness alerts."""

    def __init__(self) -> None:
        self.log = structlog.get_logger("ingest")
        self._events: Counter[str] = Counter()
        self._dlq: Counter[str] = Counter()
        self._last_event_ts: float | None = None

    def record_event(self, collection: str, *, now: float) -> None:
        self._events[collection] += 1
        self._last_event_ts = now

    def record_dlq(self, reason: str) -> None:
        self._dlq[reason] += 1

    def log_stats(self, *, now: float) -> None:
        last_age = None if self._last_event_ts is None else round(now - self._last_event_ts, 3)
        self.log.info(
            "ingest_stats",
            events=dict(self._events),
            events_total=sum(self._events.values()),
            dlq=dict(self._dlq),
            dlq_total=sum(self._dlq.values()),
            last_event_age_s=last_age,
        )
