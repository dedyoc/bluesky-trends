# Standards — bluesky-trends

## Python
- ruff format + `ruff check --fix`; mypy --strict passes. Type hints on all signatures.
- Pure functions for transforms; I/O at the edges. No business logic in Dagster/dbt config.
- Every transform gets a pytest test (happy path + one edge case). Test data as small
  fixture files, never network calls in unit tests.
- Structured logging (json) with event counts; no print().

## Streaming rules (non-negotiable)
- Ingest: persist the Jetstream cursor atomically AFTER successful Kafka produce,
  not before. Reconnect with backoff + jitter; resume from cursor.
- Producers: idempotence enabled; keys chosen for partition affinity (e.g. by DID).
- Consumers/sinks: ClickHouse writes are batched (>= 10k rows or 5s) or use async_insert. NEVER row-by-row inserts.
- All events validated against schemas/ models at the boundary; on schema mismatch,
  route to a dead-letter topic `bsky.dlq.v1` — never drop silently.

## Flink (v3)
- Event time + watermarks (bounded out-of-orderness 30s); never processing time
  for trend windows. Keyed state with TTL. Exactly-once checkpoints to MinIO.
- One job, one responsibility. New computation = new job file, not a bigger job.

## dbt / Dagster
- Layers: bronze (raw, append-only) -> staging (typed, deduped) -> marts (aggregates).
- Every model: schema tests (not_null, unique key) minimum. Marts get freshness checks.
- Backfills are Dagster partitioned jobs over Iceberg — never hand-run SQL against prod tables.

## Layout
```
ingest/  schemas/  flink/  dagster/  dbt/  api/  tests/  docker-compose.dev.yml
```

## Definition of done
lint+types pass; tests pass; schemas versioned if changed; _state.md updated.
