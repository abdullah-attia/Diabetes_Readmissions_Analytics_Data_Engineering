select *
from {{ source('raw', 'rejected') }}