-- Mart: posts per (day, lang) — one row per language per UTC day, with author counts.
-- Incremental with an explicit unique_key; re-aggregates a trailing 2-day window so late
-- bronze arrivals are absorbed without ever rebuilding full history. Reads stg_posts FINAL
-- so replayed staging duplicates never inflate the counts.

{{
  config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    order_by='(day, lang)',
    partition_by='toYYYYMM(day)',
    unique_key='(day, lang)',
    incremental_strategy='append',
  )
}}

select
    toDate(created_at)                          as day,
    arrayJoin(if(empty(langs), ['und'], langs)) as lang,
    count()                                     as posts,
    uniqExact(did)                              as authors
from {{ ref('stg_posts') }} final
{% if is_incremental() %}
    -- trailing 2-day rewindow for late data; not a full rebuild. Compare day-to-day
    -- (toDate) so the bound is unambiguous rather than relying on DateTime<->Date coercion.
    where toDate(created_at) >= (select max(day) - 2 from {{ this }})
{% endif %}
group by day, lang
