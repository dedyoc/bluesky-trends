-- Singular test: (did, rkey) is unique in the DEDUPED staging view.
-- ReplacingMergeTree collapses duplicates on merge; FINAL forces it at read time, so this
-- asserts the dedup contract even before background merges run. Returns offending rows (any
-- (did, rkey) appearing more than once after FINAL) -> dbt fails if non-empty.

select did, rkey, count() as n
from {{ ref('stg_posts') }} final
group by did, rkey
having n > 1
