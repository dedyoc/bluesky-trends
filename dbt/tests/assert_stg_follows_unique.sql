-- Singular test: (did, rkey) is unique in the DEDUPED staging view.
-- ReplacingMergeTree collapses duplicates on merge; FINAL forces it at read time, so this
-- asserts the dedup contract even before background merges run.

select did, rkey, count() as n
from {{ ref('stg_follows') }} final
group by did, rkey
having n > 1
