# Plan — v1 stage B: ClickHouse Kafka engine + MV → ReplacingMergeTree mart → Grafana

> **As-built note (2026-06-17).** This was the design draft; the shipped implementation refines
> it in three ways discovered during verification (see `.claude/outputs/stepB-verify/notes.md`
> and defects.md for the full record):
> 1. `created_at` is **Int64** on `posts_queue` (AvroConfluent does not honor timestamp-micros);
>    the MV converts to `DateTime64(6,'UTC')` via `fromUnixTimestamp64Micro`.
> 2. `format_avro_schema_registry_url` is set at **profile level** (`users.d/avro_sr.xml`) to
>    dodge the empty-MV bug; `auto_offset_reset=earliest` lives in `config.d/kafka.xml` (it is a
>    librdkafka consumer property, NOT a table SETTING).
> 3. The dev `default` user is opened over the network with an empty password
>    (`users.d/dev_default_user.xml`) so Grafana can query ClickHouse.

Consume `bsky.posts.v1` (Confluent-framed Avro) into ClickHouse via a Kafka-engine
table + materialized view landing into a `ReplacingMergeTree` mart, then surface it in
Grafana. Local-dev only (docker-compose); no k8s (that's homelab-ops).

## Scope decisions (confirmed with user)
- Mart: `ReplacingMergeTree(ingested_at)`, `ORDER BY (did, rkey)`,
  `PARTITION BY toYYYYMMDD(created_at)`. `cid` kept as a column for verification.
- Verification: full end-to-end **plus** SIGKILL-replay dedup proof (FINAL collapses dups).

## Why ClickHouse Kafka engine + MV (not a separate consumer service)
The Kafka engine table is the idiomatic, batched, async path — it reads in blocks (never
row-by-row, satisfies the standards "batched >=10k or 5s / async_insert" rule via
`kafka_max_block_size` + flush interval) and needs no extra Python service in v1. An MV is
the standard way to transform-and-land from a Kafka engine table.

**Why `AvroConfluent` format (not `Avro`)**: the producer writes the 5-byte Confluent
magic+schema-id prefix (Redpanda SR is wire-compatible, proven in stage A). `AvroConfluent`
strips that framing and resolves the writer schema from the registry; plain `Avro` would
choke on the prefix. This also matches prod (Karapace is Confluent-SR compatible).

## Why ReplacingMergeTree keyed on (did, rkey)
Matches the defects.md dedup rule ("dedupe in staging by (did, rkey/cid);
ReplacingMergeTree"). At-least-once replay after a SIGKILL produces bounded duplicate
events (proven in stage A); RMT collapses them on merge, and `FINAL` / a deduped view gives
correct reads before merges run. `ingested_at` (insert-time, `now64()` default) is the
version column so the newest landing wins — created_at is event-time and identical across
replays, so it can't serve as the version.

## What I'm NOT doing, and why
- **No `cid` in the sort key.** `(did, rkey)` uniquely identifies a record; `cid` is
  redundant for identity and would split replacement groups if a record were ever re-fetched
  with a new cid. Kept as a plain column so verification can still see it. (Defects rule says
  "did, rkey/cid" — rkey is sufficient and cleaner here.)
- **No likes/follows/dlq marts this stage.** Build order is one thing at a time; posts is
  the v1 path to Grafana. Likes/follows reuse this exact pattern later — no need to
  generalize prematurely.
- **No Iceberg / Dagster / dbt.** That's v2. Staying in v1.
- **No Flink / API.** v3.
- **No k8s manifests / image tags.** GitOps in homelab-ops; this repo is code + local dev.
- **No row-by-row or sync single-insert path.** Forbidden by standards; the Kafka-engine
  block read is the batched path by construction.
- **No DLQ-rate Grafana alerting wiring this stage.** Panel can show dlq lag later; the
  metric source (last_event_ts/counters) is an ingest concern already covered.

## Files to add/change

### 1. ClickHouse DDL — `clickhouse/init/001_posts.sql` (new)
Runs via the official image's `/docker-entrypoint-initdb.d`. Three objects in DB `bsky`:
- `posts_queue` — `ENGINE = Kafka` over `bsky.posts.v1`, `kafka_format = 'AvroConfluent'`,
  `format_avro_schema_registry_url = 'http://redpanda:8081'`, consumer group
  `clickhouse-posts`, `kafka_max_block_size` tuned so flushes are blocks not rows,
  `kafka_num_consumers = 1` (1 CH node in dev). Columns mirror the avsc exactly
  (created_at as `DateTime64(6)` ← timestamp-micros).
- `posts` — `ReplacingMergeTree(ingested_at)`, `ORDER BY (did, rkey)`,
  `PARTITION BY toYYYYMMDD(created_at)`, columns + `ingested_at DateTime64(3) DEFAULT now64(3)`.
- `posts_mv` — `MATERIALIZED VIEW ... TO posts AS SELECT ... FROM posts_queue`.
- (optional) `posts_dedup` view = `SELECT ... FROM posts FINAL` for clean reads in Grafana.

Why SQL-in-init (not a migration tool): dev parity with how Postgres init already works
(`001_init.sql` mounted into `/docker-entrypoint-initdb.d`). Same idiom, zero new tooling.

### 2. `docker-compose.dev.yml` (edit)
- Add `clickhouse` service (`clickhouse/clickhouse-server`), depends_on redpanda healthy,
  mount `./clickhouse/init` → initdb dir, healthcheck on `SELECT 1`, ports for HTTP(8123)/
  native(9000-host-remapped to avoid clash with redpanda's internal 9092? redpanda native
  is 9092/19092 — CH native 9000 is free; map 8123:8123, 9000:9000).
- Add `grafana` service, depends_on clickhouse, provisioned datasource + dashboard
  (read-only mounts), port 3000.
- Keep ingest profile-gated and untouched.

### 3. Grafana provisioning (new, under `grafana/`)
- `grafana/provisioning/datasources/clickhouse.yml` — clickhouse datasource (uses the
  grafana-clickhouse-datasource plugin; install via `GF_INSTALL_PLUGINS` env).
- `grafana/provisioning/dashboards/dashboards.yml` + `grafana/dashboards/posts.json` —
  one dashboard: posts/min (event-time), top langs, total rows. Queries hit `posts` (or
  `posts FINAL` view) — read-side dedup so duplicates from replay don't inflate panels.

### 4. `Makefile` (edit)
- `up`: add clickhouse + grafana to the infra set.
- New targets: `ch` (open clickhouse-client), `ch-count` (`SELECT count() FROM bsky.posts`
  and `count(DISTINCT (did,rkey))` to show dup collapse), `grafana` (echo the URL).

### 5. `VERIFY.md` (edit) + `.claude/outputs/stepB-verify/notes.md` (new, filled during verify)
Add stage-B checks:
- (d) events flow ingest → `bsky.posts.v1` → CH `posts` (count > 0, langs present).
- (e) **replay dedup**: note raw vs distinct count, SIGKILL ingest, restart, confirm raw
  count rises by the replay overlap but `count(DISTINCT (did,rkey))` (and `posts FINAL`)
  stays consistent → RMT collapses dups.
- (f) Grafana renders the posts dashboard from CH.

## Verification (I run all of it)
1. `make down -v` clean slate → `make up` (now incl. CH + Grafana); wait healthy.
2. `make run-ingest` ~30s; `make ch-count` → rows landing, distinct == raw (no dups yet).
3. SIGKILL ingest mid-stream, restart; `make ch-count` → raw > distinct briefly,
   `OPTIMIZE TABLE bsky.posts FINAL` (or `posts FINAL` read) → distinct == collapsed count.
4. Open Grafana (`make grafana`), confirm posts panels render.
5. `ruff format && ruff check && mypy` (no Python changed, but run for DoD) + `pytest`.
6. Fill `.claude/outputs/stepB-verify/notes.md`; update `_state.md`.

## Risks / watch-items
- **AvroConfluent ↔ Redpanda SR URL/subject**: CH must resolve schema by id from
  `http://redpanda:8081`. If subject/id lookup fails, queue table errors silently — check
  `system.kafka_consumers` / server log, surface loud in VERIFY.
- **DateTime64 precision**: created_at is micros (logicalType timestamp-micros) → CH
  `DateTime64(6)`; getting precision wrong shifts timestamps ×1000.
- **Consumer offset on dev restarts**: fixed consumer group means CH resumes; for a true
  clean-slate test use `make down -v`.
- **Grafana CH plugin install** needs network on first boot (`GF_INSTALL_PLUGINS`); note in
  VERIFY in case of air-gapped runs.

## Definition of done
lint+types+tests green; CH consumes Avro→mart; replay dedup proven; Grafana renders;
VERIFY + notes filled; `_state.md` updated. No schema changes (so no version bump needed).
