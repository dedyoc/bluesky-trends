-- Mart: follows per (day, subject_did) — one row per followed account per UTC day, with
-- distinct follower counts ("who's gaining followers"). Incremental with an explicit
-- unique_key; re-aggregates a trailing 2-day window so late bronze arrivals are absorbed
-- without rebuilding full history. Reads stg_follows FINAL so replayed staging duplicates
-- never inflate the counts.

{{
  config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    order_by='(day, subject_did)',
    partition_by='toYYYYMM(day)',
    unique_key='(day, subject_did)',
    incremental_strategy='append',
  )
}}

select
    toDate(created_at)  as day,
    subject_did,
    count()             as follows,
    uniqExact(did)      as followers
from {{ ref('stg_follows') }} final
{% if is_incremental() %}
    -- trailing 2-day rewindow for late data; not a full rebuild.
    where toDate(created_at) >= (select max(day) - 2 from {{ this }})
{% endif %}
group by day, subject_did
