# VERIFY — v1 ingest, end-to-end locally

Manual runbook to confirm the three behaviors that gate v1 sign-off:

- **(a)** real events flow into `bsky.posts.v1`
- **(b)** ingest resumes from the persisted Postgres cursor across a restart
- **(c)** a malformed event reaches `bsky.dlq.v1`

Stage B (ClickHouse sink + Grafana) adds:

- **(d)** events flow `bsky.posts.v1` → ClickHouse `bsky.posts` mart
- **(e)** SIGKILL replay duplicates collapse in the ReplacingMergeTree mart
- **(f)** Grafana renders the posts dashboard from ClickHouse

This is local-dev only (docker-compose). Nothing here deploys anything — k8s lives in
`homelab-ops`. All commands are run from the repo root.

## Topology

- **infra** (Redpanda + topic init + Postgres + ClickHouse) comes up with `make up`.
- **ingest** is gated behind the `ingest` compose profile and started separately with
  `make run-ingest`, so you can kill and restart it for the resume test **without** tearing
  down infra — the persisted cursor in Postgres must survive across ingest restarts.
- Redpanda's built-in Schema Registry (`redpanda:8081`) is Confluent-SR wire-compatible;
  Avro schemas auto-register on first produce.

## Start infra

```bash
make up
```

Wait until Redpanda is healthy and topics exist:

```bash
docker compose -f docker-compose.dev.yml exec redpanda rpk topic list
# expect: bsky.posts.v1, bsky.likes.v1, bsky.follows.v1, bsky.dlq.v1
```

---

## (a) Events flowing into bsky.posts.v1

In **terminal 1**, run ingest in the foreground:

```bash
make run-ingest
```

Watch for JSON log lines: `ingest_starting`, then `jetstream_connected`, then periodic
`ingest_stats` with `events_total` climbing and a small `last_event_age_s` (a quiet pipeline
would show a growing age — that's the staleness signal).

In **terminal 2**, consume a few Avro-framed messages:

```bash
docker compose -f docker-compose.dev.yml exec redpanda \
  rpk topic consume bsky.posts.v1 -X brokers=redpanda:9092 -n 5
```

**Observe:** five messages arrive, each with a 5-byte Confluent wire prefix
(`0x00` + 4-byte schema id) followed by the Avro body, keyed by author DID.

---

## (b) Crash-and-resume — two scenarios

> **Why two?** The service checkpoints the cursor on **graceful** shutdown, so a clean
> Ctrl-C resumes with **no** duplicates. Only a **hard kill** (SIGKILL) leaves the cursor at
> the last *periodic* checkpoint, producing the replay overlap.
>
> Mechanics: `ingest/main.py` installs SIGINT/SIGTERM handlers that set a stop event; the
> loop exits cleanly and the `finally` block runs `producer.flush(30.0)` then
> `checkpointer.final_save()`, persisting the fully-caught-up cursor. A SIGKILL skips that
> `finally` entirely, so the cursor stays at the last periodic checkpoint
> (`INGEST_CURSOR_CHECKPOINT_ACKS=100` **or** `INGEST_CURSOR_CHECKPOINT_SECONDS=2`).

Inspect the cursor at any time:

```bash
make cursor
```

### (b1) Graceful stop — NO duplicates

1. With ingest running (terminal 1), note the cursor: `make cursor` → call it **C1**.
2. **Ctrl-C** the `run-ingest` terminal. You'll see `ingest_draining` then a final
   `ingest_stats`. This is a graceful SIGINT: `flush` + `final_save` run.
   (`CMD` runs `uv` as PID 1 with no init, but Ctrl-C signals the whole process group, so
   Python's handler fires.)
3. `make cursor` again → it has advanced to **C2 > C1**: the cursor of the *latest acked*
   event, because `final_save` persisted it on the way out.
4. `make run-ingest` again. In the logs, look for:
   `ingest_starting resume_cursor=<C2>` and `jetstream_connected cursor=<C2>`.

**Observe:** a clean resume from exactly where it stopped — **no duplicate overlap**.

### (b2) Hard crash — duplicate overlap

> Keep `make run-ingest` running in terminal 1, and run the kill from **terminal 2** — a
> Ctrl-C in the foreground TTY would be graceful, not a kill.

1. Hard-kill ingest (bypasses all signal handlers; `finally`/`final_save` never runs):

   ```bash
   docker compose -f docker-compose.dev.yml kill -s SIGKILL ingest
   ```

2. `make cursor` → the cursor sits at the last **periodic** checkpoint **C3**, *behind* the
   last event actually produced (everything since the last 100-ack / 2s checkpoint is
   uncheckpointed).
3. `make run-ingest` again. Look for `ingest_starting resume_cursor=<C3>` /
   `jetstream_connected cursor=<C3>`.

**Observe:** ingest resumes from **C3**, so the events produced between C3 and the kill are
**re-produced** — the duplicate overlap, bounded by the checkpoint cadence. This is the
intended at-least-once replay; downstream sinks dedupe on `(did, rkey/cid)`.

---

## (c) Malformed event → bsky.dlq.v1

Real Jetstream data is well-formed, so the DLQ won't fire on its own. Inject one known-bad
event through the **same** validate→produce_dlq path the ingest loop uses:

```bash
make inject-dlq
```

This runs `ingest.dev_inject_dlq` inside the network: it loads
`tests/fixtures/malformed_post_event.json` (a post-create record missing the required `text`
field), runs it through `to_model` (which raises `KeyError`), and calls the real
`produce_dlq`. On success you'll see a `dlq_inject_ok` log line; the helper exits non-zero if
the message isn't delivered.

Consume it:

```bash
docker compose -f docker-compose.dev.yml exec redpanda \
  rpk topic consume bsky.dlq.v1 -X brokers=redpanda:9092 -n 1
```

**Observe:** one DLQ message whose `DlqEnvelope` carries:
- `raw_payload` — the JSON-serialized malformed event,
- `error` — the `KeyError` repr (the missing `text` field),
- `intended_topic` — `app.bsky.feed.post`,
- `received_at` — the inject time.

> **Note — the running ingest service's `dlq_total` stays 0, and that's correct.**
> `make inject-dlq` runs a *separate, short-lived* container (`docker compose run`), not the
> `make run-ingest` process. It produces a real message to `bsky.dlq.v1`, but the
> `dlq_total` you see in the service's `ingest_stats` logs is an **in-memory per-process**
> counter that only counts events *that process* DLQ'd — it never sees the out-of-band
> injection. Real Jetstream data is well-formed, so the service legitimately DLQs nothing.
> **Verify the DLQ by consuming the topic (above), not by watching `dlq_total`.** A non-zero
> `dlq_total` in the service logs would mean the live stream itself produced a malformed
> record (e.g. a lexicon change) — that's the thing worth alerting on.

---

## (d) Posts flow into the ClickHouse mart

ClickHouse comes up as part of `make up`. It runs a Kafka-engine table (`bsky.posts_queue`)
that reads `bsky.posts.v1` as `AvroConfluent`, a materialized view (`bsky.posts_mv`) that lands
into the `ReplacingMergeTree` mart (`bsky.posts`), and a `bsky.posts_dedup` view (`… FINAL`)
for clean reads. See `clickhouse/init/001_posts.sql`.

First confirm the schema-registry setting actually loaded (the load-bearing fix for the
empty-MV bug — see `clickhouse/config/users.d/avro_sr.xml`):

```bash
docker compose -f docker-compose.dev.yml exec -T clickhouse clickhouse-client --query \
  "SELECT value FROM system.settings WHERE name='format_avro_schema_registry_url'"
# expect: http://redpanda:8081
```

Run ingest for ~30s (`make run-ingest`), then:

```bash
make ch-count
```

**Observe:** `raw` and `deduped` counts both > 0 (and equal, before any replay). Spot-check a
row — `langs` populated as an array, `created_at` a real timestamp (micros scaled correctly,
not ×1000 off), and a post with no langs landing as `[]` (not a parse failure):

```bash
docker compose -f docker-compose.dev.yml exec -T clickhouse clickhouse-client --database bsky --query \
  "SELECT did, rkey, created_at, langs FROM posts ORDER BY ingested_at DESC LIMIT 5 FORMAT Vertical"
```

> If `posts_queue` has data (`SELECT count() FROM bsky.posts_queue` — note: consuming a Kafka
> engine table directly advances its offset, use sparingly) but `posts` stays empty, suspect the
> empty-SR-URL MV bug — re-check the setting query above and the CH server log for
> `Empty Schema Registry URL`.

---

## (e) Replay duplicates collapse in the mart

This consumes the bounded SIGKILL replay overlap from **(b2)** and proves the
`ReplacingMergeTree` dedupes on `(did, rkey)`.

1. With ingest running and rows landing, note `make ch-count` → `raw == deduped` (call it **N**).
2. SIGKILL ingest, then restart it (exactly the **(b2)** procedure):

   ```bash
   docker compose -f docker-compose.dev.yml kill -s SIGKILL ingest
   make run-ingest
   ```

3. After the replay, `make ch-count`:
   - `raw` has risen **above** `deduped` — the re-produced overlap landed as duplicate parts.
   - `deduped` (the `FINAL` count) stays consistent — RMT collapses the `(did, rkey)` dups.
4. Force the background merge to make the collapse physical, then re-count:

   ```bash
   docker compose -f docker-compose.dev.yml exec -T clickhouse clickhouse-client --query \
     "OPTIMIZE TABLE bsky.posts FINAL"
   make ch-count
   ```

**Observe:** after `OPTIMIZE … FINAL`, `raw == deduped` again — duplicates physically removed.

---

## (f) Grafana renders the posts dashboard

```bash
make grafana   # starts the profile-gated grafana service, prints the URL
```

Open <http://localhost:3000> (anonymous admin; no login). Open the **Bluesky posts (v1)**
dashboard.

**Observe:** the three panels render from ClickHouse — total deduped posts, posts/minute
(event-time bars), and top languages. Panels query `bsky.posts_dedup`, so the replay
duplicates from **(e)** do **not** inflate the counts.

To confirm the cross-container query path headlessly (no browser), hit Grafana's datasource
proxy directly:

```bash
curl -s -X POST http://localhost:3000/api/ds/query -H 'Content-Type: application/json' \
  -d '{"queries":[{"refId":"A","datasource":{"type":"grafana-clickhouse-datasource","uid":"bsky-clickhouse"},"rawSql":"SELECT count() FROM bsky.posts_dedup","queryType":"sql","format":1}]}'
```

> **Dev auth note.** The clickhouse-server image disables network access for the `default`
> user when no user/password env is set, so Grafana (a separate container) would get
> `Code: 516 Authentication failed`. `clickhouse/config/users.d/dev_default_user.xml` re-opens
> `default` over the network with an **empty password** (dev only; prod creds live in
> `homelab-ops`), and the provisioned datasource sends `username: default`.

---

## Teardown

```bash
make down   # stops everything AND removes volumes -> the cursor resets for a clean re-run
```
