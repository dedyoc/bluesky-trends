"""Avro Kafka producer over confluent-kafka (librdkafka).

Uses the Confluent ``AvroSerializer``, which emits the standard wire framing
``[magic 0x00][4-byte big-endian schema id][avro body]`` and auto-registers schemas
with the Schema Registry on first produce. Idempotent produce is enabled (which forces
acks=all, in-flight<=5, retries>0). DID is the message key for partition affinity.

confluent-kafka is sync/callback-based: ``produce()`` is non-blocking and the broker ack
arrives later via the delivery callback. We poll the producer to service those callbacks.
A failed delivery is surfaced through the callback so the caller never advances the cursor
past an unacked event.
"""

from __future__ import annotations

import json
import pathlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from ingest.config import Settings
from schemas.models import BskyFollow, BskyLike, BskyPost, DlqEnvelope

log = structlog.get_logger("ingest.producer")

_AVRO_DIR = pathlib.Path(__file__).resolve().parent.parent / "schemas" / "avro"

# Type of the delivery callback the caller supplies: (success, cursor) -> None.
# cursor is the Jetstream time_us carried as an opaque int through the produce call.
DeliveryCallback = Callable[[bool, int], None]


def _to_avro_dict(
    model: BskyPost | BskyLike | BskyFollow | DlqEnvelope,
    _ctx: SerializationContext,
) -> dict[str, Any]:
    """Pydantic -> plain dict for the Confluent AvroSerializer's ``to_dict`` callback.

    The serializer invokes this as ``to_dict(obj, ctx)``, so the context arg is required
    even though we don't use it. datetime stays a datetime; the timestamp-micros logical
    type handles the int conversion (tz-aware required)."""
    return model.model_dump()


class IngestProducer:
    """Routes typed models to their Avro topic; routes failures to the DLQ topic."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 100,
            }
        )
        sr = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._serializers: dict[type, tuple[str, AvroSerializer]] = {
            BskyPost: (settings.topic_posts, self._serializer(sr, "bsky.posts.v1.avsc")),
            BskyLike: (settings.topic_likes, self._serializer(sr, "bsky.likes.v1.avsc")),
            BskyFollow: (settings.topic_follows, self._serializer(sr, "bsky.follows.v1.avsc")),
            DlqEnvelope: (settings.topic_dlq, self._serializer(sr, "bsky.dlq.v1.avsc")),
        }

    @staticmethod
    def _serializer(sr: SchemaRegistryClient, avsc: str) -> AvroSerializer:
        schema_str = (_AVRO_DIR / avsc).read_text()
        serializer: AvroSerializer = AvroSerializer(sr, schema_str, _to_avro_dict)
        return serializer

    def _enqueue(
        self,
        topic: str,
        value: bytes | None,
        *,
        key: bytes | None,
        on_delivery: Callable[[Any, Any], None],
    ) -> None:
        """produce() with backpressure handling.

        librdkafka raises BufferError when the local queue is full; the fix is to poll
        (which services callbacks and drains the queue) and retry, never to drop. We retry
        a bounded number of times and re-raise if the queue stays full, so the caller can
        decide — the event is never silently lost.
        """
        for attempt in range(5):
            try:
                self._producer.produce(topic=topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:
                log.warning("produce_queue_full", topic=topic, attempt=attempt)
                self._producer.poll(0.5)
        # Final attempt; if this still raises BufferError it propagates to the caller.
        self._producer.produce(topic=topic, key=key, value=value, on_delivery=on_delivery)

    def produce(
        self,
        model: BskyPost | BskyLike | BskyFollow,
        *,
        cursor: int,
        on_delivery: DeliveryCallback,
    ) -> None:
        """Serialize and enqueue. The broker ack invokes ``on_delivery(success, cursor)``."""
        topic, serializer = self._serializers[type(model)]
        ctx = SerializationContext(topic, MessageField.VALUE)
        value = serializer(model, ctx)

        def _cb(err: Any, _msg: Any) -> None:
            on_delivery(err is None, cursor)

        self._enqueue(topic, value, key=model.did.encode("utf-8"), on_delivery=_cb)

    def produce_dlq(self, raw_payload: str, error: str, intended_topic: str) -> None:
        """Route an event that failed validation/parsing to the DLQ. Never drop silently.

        The DLQ is the last safety net, so a failed DLQ delivery is logged at error level
        (with the intended topic) rather than swallowed.
        """
        envelope = DlqEnvelope(
            raw_payload=raw_payload,
            error=error,
            intended_topic=intended_topic,
            received_at=datetime.now(tz=UTC),
        )
        topic, serializer = self._serializers[DlqEnvelope]
        ctx = SerializationContext(topic, MessageField.VALUE)
        value = serializer(envelope, ctx)

        def _cb(err: Any, _msg: Any) -> None:
            if err is not None:
                log.error("dlq_delivery_failed", intended_topic=intended_topic, error=str(err))

        self._enqueue(topic, value, key=None, on_delivery=_cb)

    def poll(self, timeout: float = 0.0) -> None:
        """Service delivery callbacks. Call regularly from the ingest loop."""
        self._producer.poll(timeout)

    def flush(self, timeout: float = 30.0) -> int:
        """Block until all in-flight messages are delivered. Returns # still queued."""
        return self._producer.flush(timeout)


def serialize_raw(event: dict[str, Any]) -> str:
    """JSON-encode a raw Jetstream event for DLQ storage (stable, sorted keys)."""
    return json.dumps(event, sort_keys=True, separators=(",", ":"))
