select
    age_group,
    min(age_mid) as age_sort
from {{ ref('fct_encounter') }}
group by age_group