select distinct payer
from {{ ref('fct_encounter') }}