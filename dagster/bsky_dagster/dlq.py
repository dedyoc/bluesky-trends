"""Minimal Avro DLQ producer for the bronze archiver.

Quarantines undecodable bronze records to ``bsky.dlq.v1`` using the same DlqEnvelope shape
and avsc as the ingest service, so a lexicon change degrades to an inspectable/replayable
record rather than a crash or a silent drop. Kept small and local (not importing the ingest
package) so the v2 layer has no dependency on ingest internals.
"""

from __future__ import annotations

import pathlib
from datetime import UTC, datetime
from typing import Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from bsky_dagster.config import Settings
from bsky_dagster.transforms.decode import DlqRow
from schemas.models import DlqEnvelope

log = structlog.get_logger("bsky_dagster.dlq")

_AVRO_DIR = pathlib.Path(__file__).resolve().parents[2] / "schemas" / "avro"


def _to_dict(model: DlqEnvelope, _ctx: Any) -> dict[str, Any]:
    """AvroSerializer to_dict callback — invoked as (model, ctx); ctx required."""
    return model.model_dump()


class DlqProducer:
    def __init__(self, settings: Settings) -> None:
        self._topic = settings.topic_dlq
        self._producer = Producer(
            {"bootstrap.servers": settings.kafka_bootstrap, "enable.idempotence": True}
        )
        sr = SchemaRegistryClient({"url": settings.schema_registry_url})
        schema_str = (_AVRO_DIR / "bsky.dlq.v1.avsc").read_text()
        self._serializer: AvroSerializer = AvroSerializer(sr, schema_str, _to_dict)

    def send(self, row: DlqRow, *, intended_topic: str) -> None:
        envelope = DlqEnvelope(
            raw_payload=row.raw_payload,
            error=row.error,
            intended_topic=intended_topic,
            received_at=datetime.now(tz=UTC),
        )
        ctx = SerializationContext(self._topic, MessageField.VALUE)
        value = self._serializer(envelope, ctx)

        def _cb(err: Any, _msg: Any) -> None:
            if err is not None:
                log.error("dlq_delivery_failed", intended_topic=intended_topic, error=str(err))

        self._producer.produce(topic=self._topic, value=value, on_delivery=_cb)
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return int(self._producer.flush(timeout))
