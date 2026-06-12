# v1 Phase 1 Plan — Jetstream Ingest + docker-compose.dev.yml
_Date: 2026-06-13_

## Scope

Build a production-quality ingest service that:
1. Consumes the Bluesky Jetstream websocket
2. Validates events against typed schemas
3. Produces Avro to Kafka (`bsky.posts.v1`, `bsky.likes.v1`, `bsky.follows.v1`)
4. Routes invalid events to `bsky.dlq.v1`
5. Persists cursor to Postgres **after** Kafka ack
6. Exports structured metrics (last_event_ts, counters)
7. Runs locally via `docker-compose.dev.yml` (Kafka + Karapace + Postgres)

Out of scope for this phase: ClickHouse, Grafana, Dagster, dbt, Flink, API.

---

## Deliverables

```
ingest/
  __init__.py
  main.py              # entrypoint: wires together config, client, producer, cursor_store
  config.py            # Settings (pydantic-settings), reads env vars
  jetstream_client.py  # Jetstream WS consumer with reconnect/backoff
  producer.py          # AIOKafkaProducer wrapper: Avro serialize, idempotence, DID key
  cursor_store.py      # Postgres UPSERT via asyncpg
  metrics.py           # Structured logger + counters (last_event_ts, events/s, dlq_count)
  dlq.py               # Dead-letter producer helper

schemas/
  __init__.py
  models.py            # Pydantic v2 models: BskyPost, BskyLike, BskyFollow, DlqEnvelope
  avro/
    bsky.posts.v1.avsc
    bsky.likes.v1.avsc
    bsky.follows.v1.avsc
    bsky.dlq.v1.avsc

tests/
  __init__.py
  test_schemas.py      # happy path + edge case for each model
  test_producer.py     # transform logic tests (no network)
  test_cursor_store.py # UPSERT logic (in-memory / mock asyncpg)
  fixtures/
    post_event.json
    like_event.json
    follow_event.json
    malformed_event.json

docker-compose.dev.yml   # Kafka (Redpanda), Karapace, Postgres
pyproject.toml           # updated with tooling config + dev deps
Dockerfile               # updated to python:3.12-slim-bookworm
```

---

## Step-by-Step Implementation

### Step 1 — pyproject.toml + tooling config

Update `pyproject.toml`:
- Add new dependencies: `asyncpg`, `pydantic-settings`, `fastavro`, `structlog`
- Add dev dependencies group: `ruff`, `mypy`, `pytest`, `pytest-asyncio`, `anyio[trio]`
- Add `[tool.ruff]` section: `target-version = "py312"`, line-length 100, select E/W/F/I/UP
- Add `[tool.mypy]` section: `strict = true`, `python_version = "3.12"`
- Add `[tool.pytest.ini_options]`: `asyncio_mode = "auto"`

### Step 2 — schemas/ package

**`schemas/models.py`** — Pydantic v2 models:
```python
class BskyPost(BaseModel):
    did: str
    rkey: str
    cid: str
    created_at: datetime
    text: str
    langs: list[str] | None = None
    reply_parent: str | None = None
    reply_root: str | None = None

class BskyLike(BaseModel):
    did: str
    rkey: str
    cid: str
    created_at: datetime
    subject_uri: str
    subject_cid: str

class BskyFollow(BaseModel):
    did: str
    rkey: str
    cid: str
    created_at: datetime
    subject_did: str

class DlqEnvelope(BaseModel):
    raw_payload: str        # json-serialized raw event
    error: str
    topic: str
    received_at: datetime
```

**Avro schemas** — mirror the Pydantic models. Fields must match exactly;
Karapace registers them on first produce.

### Step 3 — docker-compose.dev.yml

Services needed for local dev:
- **Redpanda** (Kafka-compatible, single binary, no ZK): image `redpandadata/redpanda:latest`
  - Ports: 9092 (kafka), 9644 (admin), 8081 (schema registry)
  - Pre-create topics: `bsky.posts.v1`, `bsky.likes.v1`, `bsky.follows.v1`, `bsky.dlq.v1`
- **Karapace** (schema registry): image `ghcr.io/aiven/karapace:latest`
  - Port: 8081, bootstrap → redpanda:9092
  - Note: Redpanda has a built-in schema registry on 8081; use Karapace on 8082 if both run
- **Postgres 16** (cursor store): image `postgres:16-alpine`
  - Port: 5432, db: `bsky_ingest`, user/pass: `bsky/bsky` (dev only)
  - Init SQL: create `ingest_cursors(stream_name TEXT PRIMARY KEY, cursor BIGINT, updated_at TIMESTAMPTZ)`

Environment variables exposed to `ingest` service (when added to compose later):
```
KAFKA_BOOTSTRAP=redpanda:9092
SCHEMA_REGISTRY_URL=http://karapace:8082
POSTGRES_DSN=postgresql://bsky:bsky@postgres:5432/bsky_ingest
STREAM_NAME=jetstream-main
```

### Step 4 — ingest/config.py

`pydantic-settings` `Settings` class reading from env:
```python
class Settings(BaseSettings):
    kafka_bootstrap: str = "bsky-kafka-bootstrap.kafka:9092"
    schema_registry_url: str
    postgres_dsn: PostgresDsn
    stream_name: str = "jetstream-main"
    jetstream_url: str = "wss://jetstream2.us-east.bsky.network/subscribe"
    cursor_checkpoint_interval: int = 100   # persist cursor every N successful acks
    kafka_batch_linger_ms: int = 100
```

### Step 5 — ingest/cursor_store.py

```python
async def load_cursor(pool: asyncpg.Pool, stream_name: str) -> int | None: ...
async def save_cursor(pool: asyncpg.Pool, stream_name: str, cursor: int) -> None: ...
```

`save_cursor` executes:
```sql
INSERT INTO ingest_cursors(stream_name, cursor, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (stream_name) DO UPDATE SET cursor = $2, updated_at = now()
```

Called **only** after `send_and_wait` returns without exception.

### Step 6 — ingest/producer.py

`IngestProducer` wraps `AIOKafkaProducer`:
- `enable_idempotence=True`
- `compression_type="lz4"` (reduces bandwidth to Kafka)
- Avro-serializes using `fastavro` + schema loaded from `schemas/avro/`
- Schema registration on startup via Karapace REST API
- `produce(model: BskyPost | BskyLike | BskyFollow) -> None` — sets key=`did.encode()`
- `produce_dlq(envelope: DlqEnvelope) -> None` — routes failures

### Step 7 — ingest/jetstream_client.py

`JetstreamClient`:
- Connects to `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.graph.follow&cursor={cursor}`
- Reconnect loop: exponential backoff starting 1s, cap 60s, ±20% jitter
- Yields parsed JSON event dicts; no business logic in this layer
- On connect, loads cursor from Postgres via `cursor_store.load_cursor()`

### Step 8 — ingest/metrics.py

`Metrics` — thin wrapper over `structlog` + `collections.Counter`:
```python
def record_event(collection: str) -> None: ...   # increments counter + updates last_event_ts
def record_dlq(reason: str) -> None: ...
def log_stats() -> None: ...                      # called every 10s, emits JSON log line
```

### Step 9 — ingest/main.py

Wires it all together:
1. Load `Settings`
2. Create asyncpg pool → load cursor
3. Start `IngestProducer` (registers schemas with Karapace)
4. Start `JetstreamClient`
5. For each event:
   a. Parse into appropriate Pydantic model (try BskyPost / BskyLike / BskyFollow by collection name)
   b. On `ValidationError` → `produce_dlq()`; continue
   c. On success → `producer.produce(model)` → after ack → `cursor_store.save_cursor()`
   d. Emit metrics
6. Signal handler: flush producer, close pool, log final stats

### Step 10 — tests/

- `test_schemas.py`: validate fixture JSON against each model; test that a missing required
  field raises `ValidationError`
- `test_cursor_store.py`: mock asyncpg, verify UPSERT SQL called with correct args, verify
  cursor is NOT updated when produce fails
- `test_producer.py`: test Avro round-trip for each model (serialize → deserialize)
- Fixture files: minimal valid JSON for each event type + one malformed event

### Step 11 — Dockerfile update

```dockerfile
FROM python:3.12-slim-bookworm
```
(everything else in the Dockerfile stays the same)

---

## Definition of Done (Phase 1)

- [ ] `ruff format && ruff check` passes with zero warnings
- [ ] `mypy --strict ingest/ schemas/` passes
- [ ] `pytest tests/ -v` all green
- [ ] `docker compose -f docker-compose.dev.yml up` starts Redpanda + Karapace + Postgres
- [ ] Running `uv run python -m ingest.main` locally produces events to Redpanda
      (verify with `rpk topic consume bsky.posts.v1`)
- [ ] Killing the process and restarting resumes from the persisted cursor (no replay gap > checkpoint interval)
- [ ] `bsky.dlq.v1` receives a message when a malformed event is injected
- [ ] `_state.md` updated

---

## Open Questions (resolve before executing)

1. **Jetstream URL**: Use `jetstream2.us-east.bsky.network` or the `.us-west` instance?
   Recommend us-east as primary; us-west as fallback in config.

2. **Karapace vs Redpanda built-in schema registry**: Redpanda ships its own SR on port 8081.
   Run Karapace on 8082 for local dev to match prod (where Karapace is the SR),
   or just use Redpanda's built-in for local dev simplicity?
   Recommend: use Redpanda's built-in (8081) for local dev since the wire protocol is
   identical and it removes a container.

3. **Cursor checkpoint granularity**: The config default is every 100 successful acks.
   At ~1000 events/s on Jetstream, that's a 100ms max replay window on restart.
   Is that acceptable, or should it be configurable per deployment?

4. **Avro serializer library**: `fastavro` (pure Python, fast) vs `confluent-kafka`'s
   built-in Avro serializer (requires librdkafka C binding). Recommend `fastavro` since
   we're already on `aiokafka` and want to avoid the C dependency in Docker.
