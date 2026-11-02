with e as (
    select * from {{ ref('fct_encounter') }}
    where is_readmission_eligible
)

select 'HbA1c tested' as comparison, iff(a1c_tested, 'Tested', 'Not tested') as grp,
       count(*) as eligible_encounters, count_if(readmitted_30d) as readmissions_30d,
       count_if(readmitted_30d) / count(*) as readmission_rate_30d
from e group by 2

union all

select 'HbA1c result', a1c_result,
       count(*), count_if(readmitted_30d), count_if(readmitted_30d) / count(*)
from e group by 2

union all

select 'Medication change', iff(med_changed, 'Changed', 'Not changed'),
       count(*), count_if(readmitted_30d), count_if(readmitted_30d) / count(*)
from e group by 2

union all

select 'Tested x med change',
       iff(a1c_tested, 'Tested', 'Not tested') || ' / ' || iff(med_changed, 'changed', 'not changed'),
       count(*), count_if(readmitted_30d), count_if(readmitted_30d) / count(*)
from e group by 2