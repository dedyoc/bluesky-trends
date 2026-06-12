-- Cursor store for zero-gap Jetstream resume.
-- One row per logical stream. Cursor is the Jetstream time_us (microsecond) value of
-- the last event whose Kafka produce was acked. UPSERT happens AFTER produce-ack only.
CREATE TABLE IF NOT EXISTS ingest_cursors (
    stream_name TEXT PRIMARY KEY,
    cursor      BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
