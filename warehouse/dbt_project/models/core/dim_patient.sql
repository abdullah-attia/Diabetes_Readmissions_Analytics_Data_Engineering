select
    patient_nbr,
    max(iff(is_first_encounter, race, null))       as race,
    max(iff(is_first_encounter, gender, null))     as gender,
    max(iff(is_first_encounter, age_group, null))  as first_age_group,
    count(*)                                       as encounter_count,
    sum(time_in_hospital)                          as total_bed_days,
    count(*) >= 3                                  as is_frequent_patient
from {{ ref('fct_encounter') }}
group by patient_nbr