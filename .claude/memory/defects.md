# Defects log — bluesky-trends

> Append every mistake Claude makes: [date] what — cause — rule. Newest on top.
> The entries below are PRE-LOADED known failure modes of this exact stack. Treat
> them as if they already happened to us.

## Pre-loaded platform gotchas
- ClickHouse "too many parts" / merges falling behind — Cause: many small inserts —
  Rule: batch >=10k rows or 5s, or async_insert=1; one insert block per partition where possible.
- Silent data gap after ingest restart — Cause: cursor saved before produce ack —
  Rule: persist cursor only after Kafka ack; on resume, tolerate small replay (sinks dedupe).
- Duplicate events downstream — Cause: at-least-once replay after reconnect —
  Rule: dedupe in staging by (did, rkey/cid); ReplacingMergeTree or distinct in MV.
- Pipeline silently dead, dashboards just look "quiet" — Cause: no platform metrics —
  Rule: every service exports last_event_ts + counters; alert on staleness, not just errors.
- Broken rows after Bluesky lexicon change — Cause: no schema boundary —
  Rule: validate at ingest against schemas/; unknown shapes -> DLQ topic, alert on DLQ rate.
- Flink job slow/failing checkpoints — Cause: unbounded keyed state (no TTL),
  undersized rocksdb on tiny nodes — Rule: state TTL on baselines; monitor checkpoint age.
- OOMKilled pods on homelab nodes — Cause: loading full datasets into memory —
  Rule: stream/chunk everything; set explicit memory limits in code config assumptions.
- dbt model rebuilt entire history every run — Cause: not incremental —
  Rule: marts over event data are incremental models with explicit unique_key.

## Entries
- [2026-06-13] Every Kafka produce crashed with `TypeError: _to_avro_dict() takes 1
  positional argument but 2 were given` — Cause: the Confluent `AvroSerializer` invokes its
  `to_dict` callback as `to_dict(obj, ctx)`, but `producer.py:_to_avro_dict` was defined with
  only `(model)`. Unit tests missed it because `test_avro_roundtrip.py` calls fastavro
  directly with `model.model_dump()`, never exercising the real serializer callback — only
  the end-to-end run surfaced it (high-watermark stuck at 0) — Rule: the AvroSerializer
  to_dict callback MUST take `(model, ctx)`; cover the real serializer path (not just
  fastavro round-trip) in any test claiming to verify produce. (see ingest/producer.py)
- [2026-06-13] docker-compose dev: topics silently never created + any container client
  (incl. the ingest service) could not reach Kafka — Cause: redpanda advertised a single
  `localhost:9092` listener, so in-network clients connecting to `redpanda:9092` were
  redirected to localhost and refused; the `redpanda-init` topic-create used `--brokers`
  (ignored by rpk in this image) guarded by `|| true`, masking the connection failure —
  Rule: dev redpanda needs TWO listeners with distinct advertised addrs (internal
  `redpanda:9092` for containers, external `localhost:19092` for the host); target rpk with
  `-X brokers=...` not `--brokers`; don't `|| true` topic creation — tolerate only
  ALREADY_EXISTS, fail loud otherwise. (see docker-compose.dev.yml)
- [2026-06-13] Ingest cursor watermark advanced to the *highest* acked cursor — Cause:
  out-of-order/cross-partition delivery callbacks let a later success skip past an
  earlier failed/in-flight event, persisting a cursor over a gap (silent data loss on
  restart) — Rule: advance the saved cursor only across a *contiguous* prefix of acked
  cursors; a failed delivery pins the watermark below the gap. (see ingest/main.py
  _CursorCheckpointer)
- [2026-06-13] Malformed records could escape the DLQ — Cause: parse.py raises KeyError
  on missing fields but the loop only caught ValueError/ValidationError, so a lexicon
  change would crash the loop instead of DLQ-ing — Rule: catch (ValidationError, KeyError,
  TypeError) at the parse boundary and route all to bsky.dlq.v1.
- [2026-06-13] Events with missing/non-int time_us were silently skipped (neither produced
  nor DLQ'd) — Cause: produce path guarded by `if isinstance(event_cursor, int)` with no
  else branch — Rule: an untrackable event goes to the DLQ, never dropped.
- [2026-06-13] DLQ produce had no delivery callback / BufferError handling — Cause: the
  last-resort sink could itself fail silently — Rule: every produce goes through a helper
  that retries on BufferError (poll-to-drain) and DLQ deliveries log failures at error.
