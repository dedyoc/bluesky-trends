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
- [2026-06-20] check_mart_volume gave a spurious FAIL on a single-day mart — Cause: the
  trailing-baseline query `avg(c) ... WHERE day < max(day)` runs over an EMPTY set when all
  rows share one day, and ClickHouse `avg()` over no rows returns `NaN` (not NULL/0). The
  no-baseline guard was `if not trailing:` and `not NaN` is `False` in Python, so NaN slipped
  through and `latest >= 0.25 * NaN` evaluated False -> FAIL. This was a latent bug in the
  merged posts check (posts only ever had 1 lang-day so `trailing` was falsy and it short-
  circuited); surfaced when likes/follows ran on single-day data — Rule: guard the empty-
  baseline case with `if not trailing or math.isnan(float(trailing)):` (NaN means "no trailing
  days to compare", so pass). The shared helper `_mart_volume()` now covers posts/likes/follows.
  (Separately: bronze_freshness FAILs are CORRECT when fixture data is genuinely older than
  the 24h window — that is the staleness alarm working, not a defect.)
  (see dagster/bsky_dagster/checks.py _mart_volume())
- [2026-06-18] Bronze Kafka->Iceberg asset hung forever on a re-run with no new data — Cause:
  the EOF-termination tracked partitions we'd SEEN DATA on (an `assigned` set filled only by
  data messages), so on an already-consumed topic that set stayed empty and the
  `assigned and eof>=assigned` break never fired; the loop polled None forever — Rule: derive
  "drained" from the live `consumer.assignment()` (every assigned partition still emits one
  EOF on a quiet topic with enable.partition.eof=True), not from observed-data partitions; add
  a hard idle-seconds backstop in case assignment never settles. (see
  dagster/bsky_dagster/assets/bronze.py drained())
- [2026-06-18] Dagster `@asset` rejected `def f(context: AssetExecutionContext)` with
  "Cannot annotate context parameter" — Cause: `from __future__ import annotations` stringizes
  the annotation and Dagster's runtime context-type check can't resolve the string — Rule:
  in modules with `from __future__ import annotations`, leave the asset/op `context` param
  UNannotated (or drop it), or import the real type without the future-annotations stringizing.
- [2026-06-18] PyIceberg `table.append(pyarrow_table)` raised "Mismatch in fields" — Cause:
  the pyarrow batch's field nullability and tz-awareness must EXACTLY match the Iceberg table
  schema: required Iceberg fields need non-nullable pyarrow fields, and an Iceberg
  `TimestamptzType` needs pyarrow `timestamp("us", tz="UTC")` (plain `TimestampType` ↔ tz-naive)
  — Rule: define the Iceberg `Schema` as the source of truth and mirror its required/optional +
  timestamptz in the pyarrow batch schema field-for-field. (see transforms/bronze_schema.py)
- [2026-06-17] Grafana -> ClickHouse failed with `Code: 516 Authentication failed` (HTTP 403)
  even though local clickhouse-client worked — Cause: the official clickhouse-server image
  entrypoint, when neither CLICKHOUSE_USER nor CLICKHOUSE_PASSWORD is set, logs "disabling
  network access for user 'default'" and locks `default` to localhost. Local CLI (unix/local)
  works; any OTHER container (Grafana) connecting over the network is rejected. The datasource
  also sent no username/password — Rule: in dev, explicitly open the `default` user over the
  network with an empty password via users.d (clickhouse/config/users.d/dev_default_user.xml)
  AND set `username: default` in the provisioned datasource; verify cross-container access with
  the Grafana /api/ds/query proxy, not just local clickhouse-client. (Prod creds live in
  homelab-ops, never here.)
- [2026-06-17] ClickHouse refused to start the Kafka-engine init (`Code: 115 UNKNOWN_SETTING:
  Unknown setting 'kafka_auto_offset_reset' for storage Kafka`) — Cause: assumed offset-reset
  was a `CREATE TABLE ... SETTINGS kafka_*` option; it is a librdkafka *consumer* property, set
  in a `<clickhouse><kafka><auto_offset_reset>` config-file section, not a table SETTING. (CH
  also already defaults a brand-new consumer group with no stored offset to `earliest`, so a
  clean-slate run reads the whole topic regardless.) — Rule: librdkafka tunables
  (auto.offset.reset, etc.) go in config.d/kafka.xml under `<kafka>`; only the documented
  `kafka_*` table settings (broker_list/topic_list/group_name/format/num_consumers/
  max_block_size/poll_max_batch_size/flush_interval_ms/...) belong in the SETTINGS clause.
  (see clickhouse/init/001_posts.sql + clickhouse/config/config.d/kafka.xml)
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
