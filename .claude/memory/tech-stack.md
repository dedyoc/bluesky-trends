# Tech stack — bluesky-trends

> Fill in exact versions (<REPLACE>) and keep them current. Claude must check doc
> links for APIs newer than its training data.

## Languages & tooling
- Python <REPLACE: 3.12.x>, managed with <uv | poetry>. Lint/format: ruff. Types: mypy --strict.
- (Optional Go services) Go <REPLACE>, gofmt + golangci-lint.

## Components in this repo
- **ingest/** — Jetstream websocket consumer (Bluesky atproto). Persists cursor to
  <REPLACE: Redis | Postgres | local PVC file> for zero-gap resume. Produces Avro to Kafka.
- **schemas/** — Pydantic models + Avro schemas, versioned. Single source of truth
  for event shapes; registered in schema registry <REPLACE: Confluent SR | Apicurio | Karapace>.
- **flink/** — PyFlink <REPLACE: 1.x> trend-detection job: event-time windows,
  watermarks, keyed per-topic baseline state. Checkpoints to MinIO (s3a). v3 only.
- **dagster/** — Dagster <REPLACE: 1.x> assets: Iceberg bronze -> staging -> marts,
  asset checks (freshness, volume, null-rate), backfill jobs.
- **dbt/** — dbt-core <REPLACE> with dbt-clickhouse adapter; models for marts.
- **api/** — FastAPI trends API reading ClickHouse via clickhouse-connect.

## External systems (run by homelab-ops, accessed from here)
- Kafka (Strimzi) — bootstrap: <REPLACE>. Topics: `bsky.posts.v1`, `bsky.likes.v1`, ...
- ClickHouse (Altinity operator) — hot marts on NVMe. Use async/batched inserts ONLY.
- MinIO — S3 endpoint <REPLACE>; buckets: `iceberg/`, `flink-checkpoints/`.
- Iceberg — catalog: <REPLACE: REST catalog | Nessie | Hive>.

## Build order (don't skip ahead)
v1 = ingest -> Kafka -> ClickHouse(Kafka engine + MV) -> Grafana
v2 = + Iceberg archive + Dagster/dbt batch + asset checks
v3 = + Flink trend job + FastAPI + Image Updater
Current version: <REPLACE>. Do not write v3 code while in v1 unless asked.
