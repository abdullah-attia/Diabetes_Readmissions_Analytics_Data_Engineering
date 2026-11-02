with e as (
    select * from {{ ref('fct_encounter') }}
    where is_readmission_eligible
),

segments as (
    select 'Age group'              as segment_type, age_group               as segment_value, readmitted_30d from e
    union all
    select 'Primary diagnosis',                     primary_diagnosis_group,                   readmitted_30d from e
    union all
    select 'Medical specialty',                     medical_specialty,                         readmitted_30d from e
    union all
    select 'Admission source',                      admission_source,                          readmitted_30d from e
    union all
    select 'Admission type',                        admission_type,                            readmitted_30d from e
    union all
    select 'Payer',                                 payer,                                     readmitted_30d from e
    union all
    select 'Prior inpatient visits',                prior_inpatient_band,                      readmitted_30d from e
    union all
    select 'Discharge destination',                 discharge_disposition,                     readmitted_30d from e
    union all
    select 'Gender',                                gender,                                    readmitted_30d from e
    union all
    select 'Race',                                  coalesce(race, 'Unknown'),                 readmitted_30d from e
),

overall as (
    select avg(iff(readmitted_30d, 1, 0)) as overall_rate from e
)

select
    s.segment_type,
    s.segment_value,
    count(*)                                                 as eligible_encounters,
    count_if(s.readmitted_30d)                               as readmissions_30d,
    count_if(s.readmitted_30d) / count(*)                    as readmission_rate_30d,
    (count_if(s.readmitted_30d) / count(*)) / max(o.overall_rate) as rate_vs_overall,
    count(*) < 100                                           as is_small_group
from segments s
cross join overall o
group by s.segment_type, s.segment_value