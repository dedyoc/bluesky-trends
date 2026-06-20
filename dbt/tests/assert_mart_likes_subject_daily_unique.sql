-- Singular test: (day, subject_uri) is unique in the deduped mart (FINAL).
select day, subject_uri, count() as n
from {{ ref('mart_likes_by_subject_daily') }} final
group by day, subject_uri
having n > 1
