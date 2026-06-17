# Stage B verification notes — ClickHouse sink + Grafana

_Verified 2026-06-17 on the local docker-compose stack (Redpanda + Postgres + ClickHouse 24.8 +
Grafana 11.2). All checks run by Claude end-to-end. Lint/types/tests green
(ruff format/check, mypy --strict, 42 pytest passed)._

## Topology built
`bsky.posts.v1` → `bsky.posts_queue` (ENGINE=Kafka, AvroConfluent) → `bsky.posts_mv`
(MATERIALIZED VIEW) → `bsky.posts` (ReplacingMergeTree(ingested_at), ORDER BY (did,rkey),
PARTITION BY toYYYYMMDD(created_at)). Reads via `bsky.posts_dedup` (= `SELECT * FROM posts FINAL`).

## (d) Events flow ingest → bsky.posts.v1 → ClickHouse `posts`  ✅
- SR setting loaded: `SELECT value FROM system.settings WHERE
  name='format_avro_schema_registry_url'` → `http://redpanda:8081`.
- All four objects present in `system.tables` (posts / posts_dedup / posts_mv / posts_queue).
- After ~35s of ingest: topic high-watermarks summed ~1293; `make ch-count` → `raw=1293`,
  `deduped=1293` (equal, no replay yet) — MV is NOT empty (the empty-SR-URL bug is dodged).
- Column-mapping spot-checks (the silent-failure traps):
  - `created_at` typed `DateTime64(6,'UTC')`, sample `2026-06-17 14:13:45.254000`, range
    `2026-06-15 … 2026-06-17` — micros scaled correctly (NOT ×1000 into year ~58000).
  - `langs` populated as arrays (`['en']`, `['pt']`, `['ar']`).
  - Posts with no langs land as `[]` (85 empty / 1208 non-empty), not a parse error.

## (e) SIGKILL replay duplicates collapse in the mart  ✅
- Natural replay (kill ingest mid-stream, restart from last periodic checkpoint): captured the
  intermediate state `raw=3928 > deduped=3920` — FINAL collapsed 8 replayed `(did,rkey)` dups.
  Background merges then physically collapsed (`raw==deduped` on the next read).
- Deterministic ReplacingMergeTree proof (isolates dedup from merge timing): re-inserted an
  existing `(did,rkey)` with a newer `ingested_at` and `cid=REPLAYCID`:
  - raw count for the key = **2**.
  - `... FINAL` for the key = **1**, winning `cid = REPLAYCID` → newest `ingested_at` wins
    (version column behaves correctly). Duplicates always share `created_at` → same daily
    partition, so the replacement group is always reachable.

## (f) Grafana renders the posts dashboard from ClickHouse  ✅
- Datasource provisioned: `ClickHouse` (grafana-clickhouse-datasource v4.18.0, uid
  `bsky-clickhouse`). Dashboard provisioned: `Bluesky posts (v1)` (uid `bsky-posts-v1`).
- Verified the REAL render path via Grafana's `/api/ds/query` proxy (not just local CLI):
  - Total posts panel: `SELECT count() FROM bsky.posts_dedup` → `7627`.
  - Top langs panel: → `en=4495, ja=1092, pt=247, de=217, es=189`.
- Panels query `posts_dedup` (FINAL), so replay dups never inflate counts.

## Bugs surfaced during verification (logged in defects.md, fixed)
1. `kafka_auto_offset_reset` is NOT a Kafka-storage table SETTING → `Code: 115 UNKNOWN_SETTING`,
   CH init crashed. Fixed: moved to `config.d/kafka.xml` as the librdkafka
   `<kafka><auto_offset_reset>earliest</auto_offset_reset>` consumer property (CH also defaults
   a new consumer group to earliest anyway).
2. Grafana → CH gave `Code: 516 Authentication failed` (HTTP 403) while local CLI worked —
   the image disables network access for `default` when no user/password env is set. Fixed:
   `users.d/dev_default_user.xml` opens `default` over the network with empty password (dev
   only) + `username: default` in the provisioned datasource.

## Notes
- No Python source changed (SQL/XML/YAML/JSON + Makefile only) — DoD lint/types/tests run anyway.
- ClickHouse `/var/lib/clickhouse` is ephemeral (no named volume declared), so recreating the
  CH container re-runs the init script on a clean slate — convenient for the verify loop.
