-- Mart: likes per (day, subject_uri) — one row per liked post per UTC day, with distinct
-- liker counts ("what's trending"). Incremental with an explicit unique_key; re-aggregates a
-- trailing 2-day window so late bronze arrivals are absorbed without rebuilding full history.
-- Reads stg_likes FINAL so replayed staging duplicates never inflate the counts.

{{
  config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    order_by='(day, subject_uri)',
    partition_by='toYYYYMM(day)',
    unique_key='(day, subject_uri)',
    incremental_strategy='append',
  )
}}

select
    toDate(created_at)  as day,
    subject_uri,
    count()             as likes,
    uniqExact(did)      as likers
from {{ ref('stg_likes') }} final
{% if is_incremental() %}
    -- trailing 2-day rewindow for late data; not a full rebuild.
    where toDate(created_at) >= (select max(day) - 2 from {{ this }})
{% endif %}
group by day, subject_uri
