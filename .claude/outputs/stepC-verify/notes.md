# v2 verification notes — Iceberg archive + Dagster/dbt batch + asset checks (posts)

_Verified 2026-06-18 on the local docker-compose stack (Redpanda + Postgres + ClickHouse 24.8
+ MinIO + Iceberg REST catalog 1.6.0), Dagster 1.9.13 / dagster-dbt 0.25.13 / dbt-core 1.9.8
+ dbt-clickhouse 1.9.7 / pyiceberg 0.11.1 on the host. All checks run by Claude end-to-end.
Lint/format/types/tests green: ruff (43 files), mypy --strict (43 files), pytest 55 passed
(42 v1 + 13 v2)._

## Chain built
`bsky.posts.v1` (Confluent Avro) → **posts_bronze** (Dagster asset, Kafka → Iceberg
`bronze.posts`, raw append-only, day(created_at) partitioned) → **posts_landing** (Iceberg →
ClickHouse `bsky.posts_bronze_raw`, incremental on ingest_ts) → dbt **stg_posts**
(ReplacingMergeTree(ingest_ts), deduped on (did,rkey)) → dbt **mart_posts_by_lang_daily**
(incremental, (day,lang) grain). Three Dagster asset checks + dbt not_null/unique tests.

## (g) Posts → Iceberg bronze  ✅
- PyIceberg → REST catalog → MinIO round-trip de-risked first (create/append/scan).
- `posts_bronze` materialization (partition 2026-06-17) archived **15,423** posts in ~10s,
  `dlq=0`, `bronze_total_rows=15423` — matches the topic high-watermark (~15,400).
- Decode is pure (`decode_post`): malformed → `DlqRow` → `bsky.dlq.v1`, never into bronze,
  never raised (unit-tested).

## (h) bronze → landing → staging → mart  ✅
- `posts_landing` loaded **15,423** rows Iceberg→ClickHouse (batched, never row-by-row).
- `dbt build`: **PASS=12, ERROR=0** — both incremental models + 4 not_null tests + 2 singular
  FINAL-uniqueness tests (stg + mart).
- Data correctness: `stg_posts FINAL` = **15,414** == distinct (did,rkey) in landing;
  mart `sum(posts)` = **15,729** (> staging is correct: multi-lang posts count once per lang
  via arrayJoin). Top langs en 9056 / ja 2096 / und 1563 / pt 475 / de 427 / es 398.

## (i) Bronze archival is offset-resumable  ✅
- Re-materializing `posts_bronze` with no new data archived **0** rows, `bronze_total_rows`
  unchanged at 15423 — Kafka offsets committed AFTER the Iceberg append (write-before-commit),
  so a re-run replays nothing.

## (j) At-least-once duplicates collapse in staging  ✅
- Landing held **9** duplicate (did,rkey) groups (15,423 raw vs 15,414 distinct keys); a
  bounded bronze replay. `stg_posts FINAL` = 15,414 — ReplacingMergeTree(ingest_ts) collapsed
  exactly those 9, newest landing winning.

## (k) Dagster asset checks  ✅ (run via the asset job)
- `check_bronze_freshness` (posts_bronze, WARN): passed — newest event lag 3470s (< 24h).
- `check_stg_null_rate` (stg_posts, BLOCKING/ERROR): passed — 0 empty key columns.
- `check_mart_volume` (mart, WARN): passed — latest day 76 rows vs trailing avg.
- Whole code location: `dagster definitions validate -m bsky_dagster.definitions` → successful
  (4 assets + 3 checks; dbt models wired from the manifest; `bsky_dagster` does NOT shadow the
  `dagster` pip package).

## Bugs surfaced during verification (logged in defects.md, fixed)
1. `kafka_auto_offset_reset` — n/a here (v1); see stage B.
2. **Bronze asset hung on re-run** — EOF-termination tracked seen-data partitions (empty on a
   re-run) instead of the live `consumer.assignment()`. Fixed: `drained()` uses the assignment
   + an idle-seconds backstop.
3. **Dagster `context: AssetExecutionContext` + `from __future__ import annotations`** rejected
   at definition time (stringized annotation). Fixed: drop the annotation (bronze) / drop the
   future import in the `@dbt_assets` module.
4. **PyIceberg append "Mismatch in fields"** — pyarrow batch nullability + tz-awareness must
   match the Iceberg schema exactly (required↔non-nullable; TimestamptzType↔timestamp tz=UTC).
   Fixed: Iceberg `Schema` is the source of truth; pyarrow schema mirrors it field-for-field.
5. **Landing watermark over empty table** — ClickHouse `max()` returns the epoch (not NULL) on
   an empty table, and the Iceberg `timestamptz` literal needs a zone offset. Fixed: gate on
   the landing row count; append `+00:00` if tz-naive.
6. **dbt `+schema: ""`** created a `bsky_` database (suffix concatenation). Fixed: removed the
   override so models land in `bsky` alongside the v1 tables.

## Notes / decisions
- Staging is dbt-on-ClickHouse (not PyIceberg) so the Iceberg→CH boundary is crossed once and
  dedupe reuses the v1 ReplacingMergeTree idiom.
- Bronze is day-partitioned by created_at (event time); the Dagster DailyPartitionsDefinition is
  bookkeeping — the resume cursor is the committed Kafka offset, not a partition seek.
- `created_at`/`ingest_ts` are Iceberg `timestamptz` (micros UTC). `langs` Avro null → [].
- v2 deps in the `v2` uv group; ingest Docker image still installs `--no-dev` (stays lean).
- ClickHouse `/var/lib/clickhouse` is ephemeral; `002_v2_landing.sql` is mounted so a clean
  `make down -v` recreates the landing table. No schema/avsc changes → no version bump.
