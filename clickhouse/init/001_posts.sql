-- v1 stage B: bsky.posts.v1 (Confluent-framed Avro) -> ClickHouse mart.
-- Runs once on first boot via the official image's /docker-entrypoint-initdb.d (same idiom
-- as Postgres' 001_init.sql). Topology: posts_queue (Kafka engine) -> posts_mv -> posts
-- (ReplacingMergeTree). Read through posts_dedup (FINAL) so replay duplicates never inflate.
--
-- Column types mirror schemas/avro/bsky.posts.v1.avsc exactly. Two non-obvious mappings:
--   * created_at: the Avro `long`/timestamp-micros is NOT auto-converted by AvroConfluent --
--     the Kafka-engine column must be raw Int64 micros; the MV converts to DateTime64.
--   * langs: ClickHouse cannot wrap Array in Nullable, so the Avro ["null", array] union
--     decodes into a plain Array(String) (Avro null -> empty array).

CREATE DATABASE IF NOT EXISTS bsky;

-- 1) Kafka-engine source. Reads bsky.posts.v1 in blocks (never row-by-row); the schema id in
-- the 5-byte Confluent prefix is resolved against Redpanda's SR. format_avro_schema_registry_url
-- is ALSO set at profile level (users.d/avro_sr.xml) -- the table-level setting alone does not
-- always propagate to the MV background insert thread (empty-MV bug), so both are set.
CREATE TABLE IF NOT EXISTS bsky.posts_queue
(
    did          String,
    rkey         String,
    cid          String,
    created_at   Int64,            -- raw timestamp-micros; converted in posts_mv
    text         String,
    langs        Array(String),    -- Avro null -> empty array (Array can't be Nullable)
    reply_parent Nullable(String),
    reply_root   Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'redpanda:9092',
    kafka_topic_list                = 'bsky.posts.v1',
    kafka_group_name                = 'clickhouse-posts',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://redpanda:8081',
    -- Offset reset (auto.offset.reset=earliest) is a librdkafka consumer property, NOT a
    -- table SETTING -- it's set in config/config.d/kafka.xml. ClickHouse also defaults a NEW
    -- consumer group (no stored offset) to earliest, so a clean-slate run reads the whole topic.
    kafka_num_consumers             = 1,           -- single CH node in dev
    -- Flush in blocks, not row-by-row: at most 100k rows per insert or every 5s, whichever
    -- first (satisfies the >=10k-rows-or-5s rule; avoids the "too many parts" defect).
    kafka_max_block_size            = 100000,
    kafka_poll_max_batch_size       = 10000,
    kafka_flush_interval_ms         = 5000;

-- 2) Mart. ReplacingMergeTree collapses bounded SIGKILL-replay duplicates on (did, rkey).
-- ingested_at (insert-time) is the version column so the newest landing wins -- created_at is
-- event-time and identical across replays, so it cannot version. cid is kept as a plain column
-- (verification) but NOT in the sort key: (did, rkey) is the record identity.
CREATE TABLE IF NOT EXISTS bsky.posts
(
    did          String,
    rkey         String,
    cid          String,
    created_at   DateTime64(6, 'UTC'),
    text         String,
    langs        Array(String),
    reply_parent Nullable(String),
    reply_root   Nullable(String),
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (did, rkey);

-- 3) MV transfers queue -> mart, converting micros to DateTime64. ingested_at fills via DEFAULT.
CREATE MATERIALIZED VIEW IF NOT EXISTS bsky.posts_mv TO bsky.posts AS
SELECT
    did,
    rkey,
    cid,
    fromUnixTimestamp64Micro(created_at, 'UTC') AS created_at,
    text,
    langs,
    reply_parent,
    reply_root
FROM bsky.posts_queue;

-- 4) Read-path dedup. FINAL collapses replacement groups at query time, before background
-- merges run -- correct counts even immediately after a replay. Grafana queries this view.
CREATE VIEW IF NOT EXISTS bsky.posts_dedup AS
SELECT * FROM bsky.posts FINAL;
