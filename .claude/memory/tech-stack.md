# Tech stack — bluesky-trends

> Keep versions current. Claude must check doc links for APIs newer than its training data.

## Languages & tooling
- Python 3.12.x, managed with uv. Lint/format: ruff. Types: mypy --strict.

## Components in this repo
- **ingest/** — Jetstream websocket consumer (Bluesky atproto). Persists cursor to
  Postgres (one row per stream, UPSERT after produce-ack on a periodic checkpoint — not per event) for zero-gap resume. Produces Avro to Kafka.
- **schemas/** — Pydantic models + Avro schemas, versioned. Single source of truth
  for event shapes; registered in schema registry Karapace.
- **flink/** — PyFlink 1.20.x trend-detection job: event-time windows,
  watermarks, keyed per-topic baseline state. Checkpoints to MinIO (s3a). v3 only.
- **dagster/** — Dagster 1.9.x assets: Iceberg bronze -> staging -> marts,
  asset checks (freshness, volume, null-rate), backfill jobs.
- **dbt/** — dbt-core 1.9.x with dbt-clickhouse adapter; models for marts.
- **api/** — FastAPI trends API reading ClickHouse via clickhouse-connect.

## External systems (on node0 in namespace `bsky`, accessed from here)
Manifests: `bluesky-trends-deploy`. Operators and CRDs: the `homelab` platform repo.
Exact service names are set when each slice lands there; check that repo's `docs/STATE.md`.
- Kafka (Strimzi, KRaft) — bootstrap: `<kafka-name>-kafka-bootstrap.bsky.svc:9092` (name set in slice 1). Topics: `bsky.posts.v1`, `bsky.likes.v1`, ...
- ClickHouse (Altinity operator) — hot marts on NVMe. Use async/batched inserts ONLY.
- MinIO — S3 endpoint `http://<minio-svc>.bsky.svc:9000` (name set in slice 3); buckets: `iceberg/`, `flink-checkpoints/`.
- Iceberg — catalog: REST catalog.

## Build order (don't skip ahead)
v1 = ingest -> Kafka -> ClickHouse(Kafka engine + MV) -> Grafana
v2 = + Iceberg archive + Dagster/dbt batch + asset checks
v3 = + Flink trend job + FastAPI (image digests bumped by hand in bluesky-trends-deploy; no Image Updater for now)
Current version: v1. Do not write v3 code while in v1 unless asked.
