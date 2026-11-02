with e as (
    select * from {{ ref('fct_encounter') }}
)

select
    primary_diagnosis_group,
    count(*)                                         as encounters,
    avg(time_in_hospital)                            as alos_days,
    sum(time_in_hospital)                            as bed_days,
    sum(time_in_hospital) / sum(sum(time_in_hospital)) over () as share_of_bed_days
from e
group by primary_diagnosis_group