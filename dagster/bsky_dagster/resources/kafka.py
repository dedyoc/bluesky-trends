"""Kafka consumer + Avro deserializer for the bronze archiver.

Mirrors ingest/producer.py's Confluent setup on the read side: the AvroDeserializer
resolves the 5-byte Confluent wire prefix (``[0x00][schema-id][body]``) against the
Schema Registry. The deserializer's ``from_dict`` callback signature is ``(obj, ctx)`` —
the same callback-arity contract as the producer's ``to_dict(model, ctx)`` (a real past
defect); getting it wrong fails only on the live path, so it is covered by a unit test.

Offsets are NOT auto-committed: the archiver commits only AFTER a successful Iceberg
append (write-before-commit), so Kafka's committed offset is the durable resume cursor.
"""

from __future__ import annotations

import pathlib
from typing import Any

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from bsky_dagster.config import Settings

_AVRO_DIR = pathlib.Path(__file__).resolve().parents[3] / "schemas" / "avro"


def _from_avro_dict(obj: dict[str, Any] | None, _ctx: Any) -> dict[str, Any] | None:
    """AvroDeserializer ``from_dict`` callback. Invoked as ``from_dict(obj, ctx)`` — the
    ctx arg is required even though unused. Pass the decoded dict straight through; model
    validation happens in the pure decode function, not here."""
    return obj


def build_deserializer(settings: Settings) -> AvroDeserializer:
    sr = SchemaRegistryClient({"url": settings.schema_registry_url})
    schema_str = (_AVRO_DIR / "bsky.posts.v1.avsc").read_text()
    deserializer: AvroDeserializer = AvroDeserializer(sr, schema_str, _from_avro_dict)
    return deserializer


def build_consumer(settings: Settings) -> Consumer:
    """Manual-commit consumer reading from the start for an unseen group (earliest)."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.bronze_consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,  # so a drained run can stop at EOF
        }
    )
    consumer.subscribe([settings.topic_posts])
    return consumer
