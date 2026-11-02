select
    encounter_id,
    diag_position,
    icd9_code,
    diagnosis_group
from {{ ref('stg_diagnoses') }}