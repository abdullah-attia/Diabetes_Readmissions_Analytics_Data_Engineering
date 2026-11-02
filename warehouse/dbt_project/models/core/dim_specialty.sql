select distinct medical_specialty
from {{ ref('fct_encounter') }}