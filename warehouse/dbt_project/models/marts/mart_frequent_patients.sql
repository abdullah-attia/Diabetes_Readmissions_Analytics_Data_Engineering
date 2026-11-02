with p as (
    select * from {{ ref('dim_patient') }}
),

totals as (
    select count(*) as patients, sum(encounter_count) as encounters, sum(total_bed_days) as bed_days
    from p
)

select
    iff(p.is_frequent_patient, '3+ encounters', '1-2 encounters') as patient_group,
    count(*)                                   as patients,
    sum(p.encounter_count)                     as encounters,
    sum(p.total_bed_days)                      as bed_days,
    count(*) / max(t.patients)                 as share_of_patients,
    sum(p.encounter_count) / max(t.encounters) as share_of_encounters,
    sum(p.total_bed_days) / max(t.bed_days)    as share_of_bed_days
from p
cross join totals t
group by 1