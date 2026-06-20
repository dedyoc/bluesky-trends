"""The Kafka -> Iceberg bronze follows archiver asset.

Mirror of ``bronze.py`` for the follow event type. Same streaming-safety contract: batched
writes only, write-before-commit (append to Iceberg, THEN commit Kafka offsets), undecodable
records -> bsky.dlq.v1 (never into bronze, never dropped). The generic consume helpers
(``_commit``/``_deserialize``) and the idle backstop are reused from ``bronze`` so only the
follow-specific decode/batch/schema/topic differ.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import structlog
from confluent_kafka import KafkaError

from bsky_dagster.assets.bronze import _IDLE_STOP_SECONDS, _commit, _deserialize
from bsky_dagster.config import Settings
from bsky_dagster.dlq import DlqProducer
from bsky_dagster.resources.iceberg import ensure_table, load_catalog
from bsky_dagster.resources.kafka import build_consumer, build_deserializer
from bsky_dagster.transforms.batch_follows import FollowsBatch
from bsky_dagster.transforms.bronze_schema_follows import (
    follows_iceberg_schema,
    follows_partition_spec,
)
from bsky_dagster.transforms.decode import DlqRow
from bsky_dagster.transforms.decode_follows import decode_follow
from dagster import DailyPartitionsDefinition, MaterializeResult, asset

log = structlog.get_logger("bsky_dagster.bronze_follows")

follows_daily_partitions = DailyPartitionsDefinition(start_date="2026-06-01", end_offset=1)


@asset(
    name="follows_bronze",
    partitions_def=follows_daily_partitions,
    description="Raw append-only Iceberg archive of bsky.follows.v1 (Kafka -> Iceberg bronze).",
    group_name="bronze",
)
def follows_bronze() -> MaterializeResult:
    settings = Settings()
    catalog = load_catalog(settings)
    table = ensure_table(
        catalog,
        settings,
        table_name=settings.bronze_table_follows,
        schema=follows_iceberg_schema(),
        spec=follows_partition_spec(),
    )
    deserializer = build_deserializer(settings, "bsky.follows.v1.avsc")
    consumer = build_consumer(
        settings, topic=settings.topic_follows, group=settings.bronze_consumer_group_follows
    )
    dlq = DlqProducer(settings)

    batch = FollowsBatch(settings.bronze_batch_max_rows, settings.bronze_batch_max_seconds)
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
        _commit(consumer, settings.topic_follows, batch.offsets())
        archived += len(batch)
        batch.clear()
        last_flush = time.monotonic()

    def drained() -> bool:
        """True once every CURRENTLY-ASSIGNED partition has reported EOF (see bronze.py)."""
        assignment = {tp.partition for tp in consumer.assignment()}
        return bool(assignment) and eof_partitions >= assignment

    try:
        while archived + len(batch) < settings.bronze_max_rows_per_run:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch.should_flush(elapsed_seconds=time.monotonic() - last_flush):
                    flush()
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
            row = decode_follow(
                _deserialize(deserializer, msg),
                kafka_partition=partition,
                kafka_offset=offset,
                ingest_ts=datetime.now(tz=UTC),
            )
            if isinstance(row, DlqRow):
                dlq.send(row, intended_topic=settings.topic_follows)
                dlq_count += 1
                _commit(consumer, settings.topic_follows, {partition: offset})
                continue

            batch.add(row)
            if batch.should_flush(elapsed_seconds=time.monotonic() - last_flush):
                flush()

        flush()  # drain whatever remains
    finally:
        dlq.flush()
        consumer.close()

    total = int(table.scan().to_arrow().num_rows)
    log.info("follows_bronze_done", archived=archived, dlq=dlq_count, table_total=total)
    return MaterializeResult(
        metadata={
            "archived_this_run": archived,
            "dlq_this_run": dlq_count,
            "bronze_total_rows": total,
        }
    )
