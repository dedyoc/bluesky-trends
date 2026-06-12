# v1 Code Audit — Reconciliation Report
_Date: 2026-06-13_

## Summary

The existing codebase is a single-file prototype (`src/main.py`) that connects to the
Bluesky **Firehose** (not Jetstream) and produces JSON to Kafka. It diverges from the
target architecture in nearly every dimension. The repo also lacks the required directory
structure, Postgres cursor store, schemas package, tooling configuration, and
docker-compose for local dev.

---

## Mismatches

### 1. Wrong Bluesky client — Firehose vs Jetstream

| | Required | Actual |
|---|---|---|
| Client | `AsyncJetstreamClient` (atproto Jetstream websocket) | `AsyncFirehoseSubscribeReposClient` (Firehose CBORhose) |
| Protocol | Jetstream (JSON over WS, built-in cursor param) | `com.atproto.sync.subscribeRepos` (binary CBOR/CAR) |

**Impact:** The entire message parsing pipeline (`CAR.from_bytes`, `parse_subscribe_repos_message`,
`ComAtprotoSyncSubscribeRepos.Commit`) is Firehose-specific and will be replaced with
Jetstream. The cursor mechanism also differs (Jetstream uses a microsecond-timestamp
cursor via URL param; Firehose uses `seq`).

**What needs changing:** Replace the client and message-handling loop entirely with the
Jetstream client. Remove CAR/CBORhose imports.

---

### 2. Kafka bootstrap server wrong

| | Required | Actual |
|---|---|---|
| Bootstrap | `bsky-kafka-bootstrap.kafka:9092` | `homelab-broker-kafka-bootstrap.kafka-cluster.svc:9092` |

**What needs changing:** Update `KAFKA_BOOTSTRAP_SERVERS` constant.

---

### 3. Kafka topic name wrong

| | Required | Actual |
|---|---|---|
| Posts topic | `bsky.posts.v1` | `bluesky-posts` |

**What needs changing:** Rename `KAFKA_TOPIC` to `bsky.posts.v1`. (Plus `bsky.likes.v1`
and `bsky.follows.v1` topics will be needed once schemas are defined.)

---

### 4. Message format — JSON instead of Avro

| | Required | Actual |
|---|---|---|
| Wire format | Avro (serialized via schema registry Karapace) | `json.dumps(...).encode('utf-8')` |
| Schema registry | Karapace | Not used |
| Schema definition | In `schemas/` package | Inline ad-hoc dict |

**What needs changing:** Add `schemas/` package with Pydantic models + Avro schemas.
Wire up `confluent-kafka` or `aiokafka` with Karapace-compatible Avro serializer. All
Kafka-bound data must use typed models from `schemas/`, not ad-hoc dicts.

---

### 5. No Postgres cursor store

| | Required | Actual |
|---|---|---|
| Cursor persistence | Postgres (UPSERT after Kafka produce-ack, not per-event) | In-memory only via `client.update_params()` every 20 seq |

**Impact:** Every restart loses cursor position; ingest replays from the beginning or
drops data silently depending on the Jetstream server's retention. This is the exact
"silent data gap after ingest restart" gotcha in defects.md.

**What needs changing:** Add `asyncpg` dependency and a `cursor_store` module that
UPSERTs the cursor to a `ingest_cursors` table after each successful `send_and_wait`.

---

### 6. Cursor update logic is wrong even for current code

The current code calls `client.update_params(cursor=commit.seq)` every 20 messages,
**before** the Kafka produce. If the produce fails or the process crashes mid-batch,
the cursor is already advanced and events are silently lost. This violates the
"persist cursor only after Kafka ack" rule and directly matches defects.md entry
"Silent data gap after ingest restart."

**What needs changing:** Move cursor persistence to after `send_and_wait` succeeds,
not before.

---

### 7. No Kafka producer idempotence

| | Required | Actual |
|---|---|---|
| Idempotence | `enable.idempotence=True` | Not set |
| Partition key | By DID for partition affinity | `send_and_wait(topic, value=...)` — no key |

**What needs changing:** Set `enable_idempotence=True` on `AIOKafkaProducer`.
Use DID (`author`) as the partition key so all events from one account land on the
same partition.

---

### 8. No schema validation / DLQ routing

| | Required | Actual |
|---|---|---|
| Schema validation | All events validated against `schemas/` at boundary | No validation |
| Bad event handling | Route to `bsky.dlq.v1` | Silently dropped or crashes |

**What needs changing:** After Jetstream message parsing, validate against the Pydantic
model from `schemas/`. On `ValidationError`, produce to `bsky.dlq.v1` with the raw
payload and error details.

---

### 9. Structured logging — `print()` instead of JSON logger

| | Required | Actual |
|---|---|---|
| Logging | `structlog` or `logging` with JSON formatter, event counts | `print(f'...')` throughout |
| Metrics | `last_event_ts` + counters exported | None |

Every `print()` in the file (`'NETWORK LOAD: ...'`, `'shutting down gracefully...'`,
`'kafka delivery failure: ...'`) violates the "no print()" standard.

**What needs changing:** Replace all `print()` calls with a structured JSON logger.
Add a counter that logs events-per-second via the logger, not stdout.

---

### 10. Missing type annotations / mypy compliance

The `measure_events_per_second` function:
- `func: callable` — `callable` is lowercase (not `Callable`), will fail mypy --strict
- `wrapper.calls` and `wrapper.start_time` are attribute mutations on a function object
  without type stubs — fails mypy --strict
- `def wrapper(*args) -> Any` — `args` is untyped

**What needs changing:** Replace the decorator with a proper typed class or a `Counter`
approach. Under mypy --strict, the current pattern is invalid.

---

### 11. Dockerfile uses Python 3.11, not 3.12

| | Required | Actual |
|---|---|---|
| Python version | 3.12 (`.python-version` = `3.12`) | `FROM python:3.11-slim-bookworm` |

**What needs changing:** Change base image to `python:3.12-slim-bookworm`.

---

### 12. Missing `pyproject.toml` tooling configuration

| | Required | Actual |
|---|---|---|
| ruff config | Needed for `ruff format` + `ruff check --fix` | Not present in pyproject.toml |
| mypy config | `mypy --strict` section | Not present |
| pytest config | test runner config | Not present |
| dev dependencies | ruff, mypy, pytest | Not in pyproject.toml |

**What needs changing:** Add `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`
sections and a `[dependency-groups]` dev group with ruff, mypy, pytest, pytest-asyncio.

---

### 13. Missing required directory structure

Standards require:
```
ingest/  schemas/  flink/  dagster/  dbt/  api/  tests/  docker-compose.dev.yml
```

Actual layout has only `src/main.py` — a flat prototype. No `ingest/`, `schemas/`,
`tests/`, or `docker-compose.dev.yml`.

**What needs changing:** Restructure to the required layout before v1 is complete.
`src/main.py` should become `ingest/` package. `schemas/` package is required for
Avro/Kafka boundary. `tests/` must exist with at minimum happy-path + edge-case tests
for the transform functions.

---

### 14. No reconnect with backoff + jitter

The current code has no explicit reconnect logic. `client.start()` will bubble up
exceptions on disconnect; there is no retry loop with exponential backoff + jitter.

**What needs changing:** Wrap the client start loop in a retry with
`asyncio.sleep(backoff_with_jitter)` on transient errors.

---

## Items That Are Correct (keep as-is)

- `uv` is used as the package manager — correct.
- `requires-python = ">=3.12"` in `pyproject.toml` — correct.
- `aiokafka>=0.14.0` — appropriate async Kafka client.
- `uv sync --frozen` in Dockerfile — correct lockfile discipline.
- Signal handling pattern (`SIGINT`/`SIGTERM` → graceful shutdown) — correct structure,
  though it uses `print()` which needs to change.
- Filtering to only `create` ops (skipping `update`) — semantically correct for posts.
- Using `send_and_wait` (waits for broker ack) — correct foundation; just needs
  idempotence and cursor-after-ack ordering.

---

## Priority Order for Fixes

1. **Directory restructure** — everything else builds on this layout
2. **schemas/ package** — Pydantic models + Avro schemas; required before any Kafka produce changes
3. **Switch Firehose → Jetstream** client
4. **Kafka bootstrap + topic names**
5. **Kafka producer idempotence + DID partition key**
6. **Postgres cursor store** (UPSERT after ack)
7. **Avro serialization** via Karapace
8. **DLQ routing** for schema mismatches
9. **Structured logging** — replace all print()
10. **Reconnect with backoff + jitter**
11. **pyproject.toml tooling** (ruff/mypy/pytest config + dev deps)
12. **Dockerfile Python 3.11 → 3.12**
13. **Tests** — transform unit tests
14. **docker-compose.dev.yml**
