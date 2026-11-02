select
    encounter_id,
    medication,
    status
from {{ ref('stg_medications') }}