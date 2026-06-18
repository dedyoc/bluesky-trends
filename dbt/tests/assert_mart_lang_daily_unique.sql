-- Singular test: (day, lang) is unique in the deduped mart (FINAL).
select day, lang, count() as n
from {{ ref('mart_posts_by_lang_daily') }} final
group by day, lang
having n > 1
