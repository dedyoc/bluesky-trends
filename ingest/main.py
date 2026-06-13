"""Ingest service entrypoint.

Flow: load cursor from Postgres -> connect Jetstream from that cursor -> for each event,
parse into a typed model and produce Avro to Kafka (DID-keyed). On a broker ack, advance
an in-memory "acked cursor"; checkpoint it to Postgres after N acks OR T seconds, whichever
first. Malformed events go to the DLQ, never dropped. Cursor is persisted only AFTER ack.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time

import structlog
from pydantic import ValidationError

from ingest.config import Settings
from ingest.cursor_store import CursorStore
from ingest.jetstream_client import JetstreamClient
from ingest.metrics import Metrics
from ingest.parse import to_model
from ingest.producer import IngestProducer, serialize_raw

log = structlog.get_logger("ingest.main")


class _CursorCheckpointer:
    """Computes a gap-free safe cursor watermark and flushes it on the configured cadence.

    Persisting the *highest* acked cursor is unsafe: delivery callbacks can fire out of
    order (and across partitions), so a later event's success must not advance the cursor
    past an earlier event that failed or is still in flight — that would skip the gap and
    cause silent data loss on restart. Instead we track every produced cursor as
    "outstanding" and advance the watermark only across a contiguous prefix of acked
    cursors. A failed delivery is left outstanding forever, which permanently pins the
    watermark below the gap; on restart we replay from there (sinks dedupe).
    """

    def __init__(self, store: CursorStore, settings: Settings) -> None:
        self._store = store
        self._settings = settings
        # Outstanding produced cursors not yet folded into the watermark, with ack state.
        self._pending_acks: dict[int, bool] = {}
        self._safe_cursor: int | None = None
        self._saved_cursor: int | None = None
        self._acks_since_save = 0
        self._last_save_monotonic = time.monotonic()

    def register(self, cursor: int) -> None:
        """Record a cursor as produced-but-unacked. Call BEFORE/at produce time."""
        self._pending_acks.setdefault(cursor, False)

    def on_ack(self, success: bool, cursor: int) -> None:
        """Delivery callback. Marks the cursor acked; failures stay outstanding (pin gap)."""
        if not success:
            log.error("produce_failed", cursor=cursor)
            return
        if cursor in self._pending_acks:
            self._pending_acks[cursor] = True
        self._advance_watermark()

    def _advance_watermark(self) -> None:
        """Fold the longest contiguous run of acked cursors (ascending) into the watermark.

        Stops at the first cursor that is still unacked or failed, so the watermark never
        crosses a gap.
        """
        for cursor in sorted(self._pending_acks):
            if not self._pending_acks[cursor]:
                break  # gap: an earlier event is unacked/failed — stop here.
            self._safe_cursor = cursor
            del self._pending_acks[cursor]
            self._acks_since_save += 1

    def _due(self, now: float) -> bool:
        if self._safe_cursor is None or self._safe_cursor == self._saved_cursor:
            return False
        if self._acks_since_save >= self._settings.cursor_checkpoint_acks:
            return True
        return (now - self._last_save_monotonic) >= self._settings.cursor_checkpoint_seconds

    async def maybe_save(self, *, now: float) -> None:
        if not self._due(now):
            return
        assert self._safe_cursor is not None
        await self._store.save(self._settings.stream_name, self._safe_cursor)
        self._saved_cursor = self._safe_cursor
        self._acks_since_save = 0
        self._last_save_monotonic = now

    async def final_save(self) -> None:
        if self._safe_cursor is not None and self._safe_cursor != self._saved_cursor:
            await self._store.save(self._settings.stream_name, self._safe_cursor)
            self._saved_cursor = self._safe_cursor


async def run(settings: Settings, stop: asyncio.Event) -> None:
    metrics = Metrics()
    store = await CursorStore.connect(settings.postgres_dsn)
    producer = IngestProducer(settings)
    client = JetstreamClient(settings)
    checkpointer = _CursorCheckpointer(store, settings)

    cursor = await store.load(settings.stream_name)
    log.info("ingest_starting", stream=settings.stream_name, resume_cursor=cursor)

    last_stats = time.monotonic()
    try:
        async for event in client.events(cursor):
            if stop.is_set():
                break
            now = time.monotonic()
            event_cursor = event.get("time_us")
            commit = event.get("commit")
            collection = (
                commit.get("collection", "unknown") if isinstance(commit, dict) else "unknown"
            )

            try:
                model = to_model(event)
            except ValueError:
                # Not an ingestable shape (delete, account, unknown collection) — skip quietly.
                pass
            except (ValidationError, KeyError, TypeError) as exc:
                # Malformed/lexicon-changed record: missing or wrong-typed fields. Never drop;
                # route to the DLQ so the rate can be alerted on and the event replayed.
                producer.produce_dlq(
                    serialize_raw(event), repr(exc), intended_topic=str(collection)
                )
                metrics.record_dlq(reason=str(collection))
            else:
                if not isinstance(event_cursor, int):
                    # A parseable event with no usable time_us cursor would have to be
                    # produced without an ack-trackable cursor — DLQ it rather than drop.
                    producer.produce_dlq(
                        serialize_raw(event),
                        "missing or non-int time_us",
                        intended_topic=str(collection),
                    )
                    metrics.record_dlq(reason="missing_time_us")
                else:
                    # Register BEFORE produce so the watermark can never skip this cursor.
                    checkpointer.register(event_cursor)
                    producer.produce(model, cursor=event_cursor, on_delivery=checkpointer.on_ack)
                    metrics.record_event(collection, now=time.time())

            producer.poll(0)
            await checkpointer.maybe_save(now=now)

            if now - last_stats >= settings.stats_interval_seconds:
                metrics.log_stats(now=time.time())
                last_stats = now
    finally:
        log.info("ingest_draining")
        producer.flush(30.0)
        await checkpointer.final_save()
        await store.close()
        metrics.log_stats(now=time.time())


def main() -> None:
    settings = Settings()
    stop = asyncio.Event()

    async def _amain() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await run(settings, stop)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_amain())


if __name__ == "__main__":
    main()
