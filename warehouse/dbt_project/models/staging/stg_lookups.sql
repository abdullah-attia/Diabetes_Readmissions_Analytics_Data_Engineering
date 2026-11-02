select
    mapping_type,
    id::varchar   as id,
    label
from {{ source('raw', 'lookups') }}