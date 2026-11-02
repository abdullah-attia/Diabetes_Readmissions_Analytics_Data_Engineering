with e as (
    select * from {{ ref('fct_encounter') }}
),

cost as (
    select value::number(12, 2) as cost_per_day
    from {{ ref('cost_assumptions') }}
    where assumption = 'cost_per_inpatient_day'
),

k as (
    select
        count(*)                                                        as total_encounters,
        count(distinct patient_nbr)                                     as total_patients,
        count_if(is_readmission_eligible)                               as eligible_encounters,
        count_if(is_readmission_eligible and readmitted_30d)            as readmissions_30d,
        count_if(is_readmission_eligible and readmitted in ('<30', '>30')) as readmissions_any,
        avg(time_in_hospital)                                           as alos_days,
        sum(time_in_hospital)                                           as bed_days,
        count_if(a1c_tested)                                            as a1c_tested_encounters,
        count_if(is_first_encounter and is_readmission_eligible)        as benchmark_patients,
        count_if(is_first_encounter and is_readmission_eligible and readmitted_30d) as benchmark_readmissions
    from e
)

select
    k.*,
    k.readmissions_30d / nullif(k.eligible_encounters, 0)           as readmission_rate_30d,
    k.readmissions_any / nullif(k.eligible_encounters, 0)           as readmission_rate_any,
    k.a1c_tested_encounters / nullif(k.total_encounters, 0)         as a1c_testing_rate,
    k.benchmark_readmissions / nullif(k.benchmark_patients, 0)      as benchmark_rate_30d,
    cost.cost_per_day,
    k.readmissions_30d * k.alos_days * cost.cost_per_day            as est_readmission_cost
from k
cross join cost