with enc as (
    select * from {{ ref('stg_encounters') }}
),

primary_dx as (
    select encounter_id, diagnosis_group as primary_diagnosis_group
    from {{ ref('stg_diagnoses') }}
    where diag_position = 1
)

select
    enc.encounter_id,
    enc.patient_nbr,
    enc.encounter_seq,
    enc.is_first_encounter,

    -- dimension keys (labels)
    coalesce(enc.admission_type, 'Unknown')         as admission_type,
    coalesce(enc.admission_source, 'Unknown')       as admission_source,
    coalesce(enc.discharge_disposition, 'Unknown')  as discharge_disposition,
    coalesce(enc.medical_specialty, 'Unknown')      as medical_specialty,
    coalesce(enc.payer, 'Unknown')                  as payer,
    coalesce(enc.age_group, 'Unknown')              as age_group,
    coalesce(dx.primary_diagnosis_group, 'Unknown') as primary_diagnosis_group,

    -- patient attributes at this encounter
    enc.race,
    enc.gender,
    enc.age_mid,

    -- measures
    enc.time_in_hospital,
    enc.num_lab_procedures,
    enc.num_procedures,
    enc.num_medications,
    enc.number_outpatient,
    enc.number_emergency,
    enc.number_inpatient,
    enc.number_diagnoses,
    case
        when enc.number_inpatient = 0 then '0'
        when enc.number_inpatient = 1 then '1'
        when enc.number_inpatient = 2 then '2'
        else '3+'
    end                                             as prior_inpatient_band,

    -- care quality
    enc.a1c_result,
    enc.a1c_tested,
    enc.glucose_result,
    enc.med_changed,
    enc.on_diabetes_med,

    -- outcome
    enc.is_readmission_eligible,
    enc.readmitted,
    enc.readmitted_30d

from enc
left join primary_dx dx
    on dx.encounter_id = enc.encounter_id