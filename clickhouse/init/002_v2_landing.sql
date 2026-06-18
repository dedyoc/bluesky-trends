-- v2 landing table: the Dagster landing asset batches new Iceberg bronze rows into here,
-- and the dbt `stg_posts` model reads from it. Append-only MergeTree (raw landing; dedupe
-- happens in staging via ReplacingMergeTree on (did, rkey)). Inserts are batched by the
-- landing asset (async/large blocks), never row-by-row.
--
-- Mirrors the bronze Iceberg columns (schemas/avro/bsky.posts.v1.avsc + kafka provenance).
-- created_at/ingest_ts are DateTime64(6,'UTC') micros; langs is Array(String) (Avro null -> []).

CREATE DATABASE IF NOT EXISTS bsky;

CREATE TABLE IF NOT EXISTS bsky.posts_bronze_raw
(
    did             String,
    rkey            String,
    cid             String,
    created_at      DateTime64(6, 'UTC'),
    text            String,
    langs           Array(String),
    reply_parent    Nullable(String),
    reply_root      Nullable(String),
    kafka_partition Int32,
    kafka_offset    Int64,
    ingest_ts       DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (did, rkey, ingest_ts);
