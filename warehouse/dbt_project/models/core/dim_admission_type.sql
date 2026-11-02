select distinct admission_type
from {{ ref('fct_encounter') }}