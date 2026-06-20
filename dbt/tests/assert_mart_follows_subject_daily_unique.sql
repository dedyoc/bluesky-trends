-- Singular test: (day, subject_did) is unique in the deduped mart (FINAL).
select day, subject_did, count() as n
from {{ ref('mart_follows_by_subject_daily') }} final
group by day, subject_did
having n > 1
