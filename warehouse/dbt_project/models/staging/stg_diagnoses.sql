select
    encounter_id::varchar   as encounter_id,
    diag_position::int      as diag_position,
    icd9_code,
    diagnosis_group
from {{ source('raw', 'diagnoses') }}