-- Staging: typed + deduped follows, one row per (did, rkey).
-- Reuses the v1 dedupe idiom: ReplacingMergeTree(ingest_ts) collapses replayed bronze rows
-- (the bronze archiver is at-least-once) keeping the newest landing. Incremental on ingest_ts
-- so each run only reads newly-landed rows — never a full-history rebuild.

{{
  config(
    materialized='incremental',
    engine='ReplacingMergeTree(ingest_ts)',
    order_by='(did, rkey)',
    partition_by='toYYYYMM(created_at)',
    unique_key='(did, rkey)',
    incremental_strategy='append',
  )
}}

select
    did,
    rkey,
    cid,
    created_at,
    subject_did,
    ingest_ts
from {{ source('bsky', 'follows_bronze_raw') }}

{% if is_incremental() %}
    -- only rows landed since the latest one already in staging
    where ingest_ts > (select max(ingest_ts) from {{ this }})
{% endif %}
