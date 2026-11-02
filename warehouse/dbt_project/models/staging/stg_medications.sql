select
    encounter_id::varchar   as encounter_id,
    medication,
    status
from {{ source('raw', 'medications') }}