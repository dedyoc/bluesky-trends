"""The Kafka -> Iceberg bronze archiver asset.

This is the one I/O edge that owns the consume loop. It reuses the pure transforms
(decode -> BronzeBatch -> pyarrow Table) and enforces the streaming-safety contract:

* Batched writes only (BronzeBatch flushes at >=N rows or T seconds) — never row-by-row.
* Write-before-commit: append to Iceberg, THEN commit Kafka offsets. A crash in between
  replays a bounded batch on the next run; bronze is append-only so dedupe happens at
  staging (the mirror of v1's "ack before cursor" rule).
* Undecodable records -> bsky.dlq.v1 (never into bronze, never dropped), counted.

The asset is logically day-partitioned (DailyPartitionsDefinition) for downstream checks
and backfill bookkeeping; the actual resume point is the Kafka committed offset, not a
partition seek.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import structlog
from confluent_kafka import KafkaError, TopicPartition

from bsky_dagster.config import Settings
from bsky_dagster.dlq import DlqProducer
from bsky_dagster.resources.iceberg import ensure_table, load_catalog
from bsky_dagster.resources.kafka import build_consumer, build_deserializer
from bsky_dagster.transforms.batch import BronzeBatch
from bsky_dagster.transforms.bronze_schema import bronze_iceberg_schema, partition_spec
from bsky_dagster.transforms.decode import DlqRow, decode_post
from dagster import DailyPartitionsDefinition, MaterializeResult, asset

log = structlog.get_logger("bsky_dagster.bronze")

# Backstop: stop a run after this long with no message at all (no data, no EOF) — guards
# against an assignment that never settles. Normal termination is partition-EOF (drained()).
_IDLE_STOP_SECONDS = 15.0

# end_offset=1 includes the current (in-progress) day as a materializable partition; the
# bronze consume reads from the committed Kafka offset regardless of which day key is run,
# so the partition is bookkeeping for downstream checks/backfills, not a Kafka seek.
posts_daily_partitions = DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1)


@asset(
    name="posts_bronze",
    partitions_def=posts_daily_partitions,
    description="Raw append-only Iceberg archive of bsky.posts.v1 (Kafka -> Iceberg bronze).",
    group_name="bronze",
)
def posts_bronze() -> MaterializeResult:
    settings = Settings()
    catalog = load_catalog(settings)
    table = ensure_table(
        catalog,
        settings,
        table_name=settings.bronze_table,
        schema=bronze_iceberg_schema(),
        spec=partition_spec(),
    )
    deserializer = build_deserializer(settings, "bsky.posts.v1.avsc")
    consumer = build_consumer(
        settings, topic=settings.topic_posts, group=settings.bronze_consumer_group
    )
    dlq = DlqProducer(settings)

    batch = BronzeBatch(settings.bronze_batch_max_rows, settings.bronze_batch_max_seconds)
    archived = 0
    dlq_count = 0
    last_flush = time.monotonic()
    last_activity = time.monotonic()
    eof_partitions: set[int] = set()

    def flush() -> None:
        nonlocal archived, last_flush
        if batch.is_empty():
            return
        table.append(batch.to_arrow())  # write FIRST
        # then commit the per-partition high-water offset (write-before-commit).
        _commit(consumer, settings.topic_posts, batch.offsets())
        archived += len(batch)
        batch.clear()
        last_flush = time.monotonic()

    def drained() -> bool:
        """True once every CURRENTLY-ASSIGNED partition has reported EOF. Uses the live
        assignment (not "partitions we saw data on"), so a quiet/already-consumed topic
        terminates: with no new data, each assigned partition still emits one EOF."""
        assignment = {tp.partition for tp in consumer.assignment()}
        return bool(assignment) and eof_partitions >= assignment

    try:
        while archived + len(batch) < settings.bronze_max_rows_per_run:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch.should_flush(elapsed_seconds=time.monotonic() - last_flush):
                    flush()
                # Stop when the topic is drained, or after a hard idle ceiling as a backstop
                # (e.g. assignment never settles).
                idle = time.monotonic() - last_activity
                if batch.is_empty() and (drained() or idle > _IDLE_STOP_SECONDS):
                    break
                continue

            last_activity = time.monotonic()
            err = msg.error()
            partition = msg.partition()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF and partition is not None:
                    eof_partitions.add(partition)
                    continue
                raise RuntimeError(f"kafka consume error: {err}")

            offset = msg.offset()
            if partition is None or offset is None:  # never for a real data message
                continue

            eof_partitions.discard(partition)  # new data => no longer at EOF
            row = decode_post(
                _deserialize(deserializer, msg),
                kafka_partition=partition,
                kafka_offset=offset,
                ingest_ts=datetime.now(tz=UTC),
            )
            if isinstance(row, DlqRow):
                dlq.send(row, intended_topic=settings.topic_posts)
                dlq_count += 1
                # Commit past the bad message so a rerun doesn't re-DLQ it.
                _commit(consumer, settings.topic_posts, {partition: offset})
                continue

            batch.add(row)
            if batch.should_flush(elapsed_seconds=time.monotonic() - last_flush):
                flush()

        flush()  # drain whatever remains
    finally:
        dlq.flush()
        consumer.close()

    total = int(table.scan().to_arrow().num_rows)
    log.info("posts_bronze_done", archived=archived, dlq=dlq_count, table_total=total)
    return MaterializeResult(
        metadata={
            "archived_this_run": archived,
            "dlq_this_run": dlq_count,
            "bronze_total_rows": total,
        }
    )


def _commit(consumer: Any, topic: str, offsets: dict[int, int]) -> None:
    """Commit the next-to-read offset (committed = last consumed + 1) per partition."""
    if not offsets:
        return
    consumer.commit(
        offsets=[TopicPartition(topic, p, o + 1) for p, o in offsets.items()],
        asynchronous=False,
    )


def _deserialize(deserializer: Any, msg: Any) -> dict[str, Any] | None:
    """Avro-decode one message value; None on any deserialization failure (-> DLQ path)."""
    from confluent_kafka.serialization import MessageField, SerializationContext

    try:
        ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        result: dict[str, Any] | None = deserializer(msg.value(), ctx)
        return result
    except Exception:  # noqa: BLE001 — any decode failure is a DLQ candidate, never a crash
        return None
