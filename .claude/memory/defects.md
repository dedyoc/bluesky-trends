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
- (none yet)
