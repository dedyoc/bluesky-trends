-- v2 landing tables for likes + follows: the Dagster likes_landing/follows_landing assets
-- batch new Iceberg bronze rows into here, and the dbt stg_likes/stg_follows models read from
-- them. Append-only MergeTree (raw landing; dedupe happens in staging via
-- ReplacingMergeTree on (did, rkey)). Inserts are batched by the landing assets (large
-- blocks), never row-by-row.
--
-- Mirrors the bronze Iceberg columns (schemas/avro/bsky.likes.v1.avsc /
-- bsky.follows.v1.avsc + kafka provenance). created_at/ingest_ts are DateTime64(6,'UTC')
-- micros. Unlike posts, a like/follow has no nullable payload fields.

CREATE DATABASE IF NOT EXISTS bsky;

CREATE TABLE IF NOT EXISTS bsky.likes_bronze_raw
(
    did             String,
    rkey            String,
    cid             String,
    created_at      DateTime64(6, 'UTC'),
    subject_uri     String,
    subject_cid     String,
    kafka_partition Int32,
    kafka_offset    Int64,
    ingest_ts       DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (did, rkey, ingest_ts);

CREATE TABLE IF NOT EXISTS bsky.follows_bronze_raw
(
    did             String,
    rkey            String,
    cid             String,
    created_at      DateTime64(6, 'UTC'),
    subject_did     String,
    kafka_partition Int32,
    kafka_offset    Int64,
    ingest_ts       DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (did, rkey, ingest_ts);
