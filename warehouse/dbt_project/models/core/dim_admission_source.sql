select distinct admission_source
from {{ ref('fct_encounter') }}