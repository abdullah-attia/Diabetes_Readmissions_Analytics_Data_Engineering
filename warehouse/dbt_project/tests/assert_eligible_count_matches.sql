select 1
from {{ ref('mart_readmission_kpis') }}
where eligible_encounters <> (
    select count(*) from {{ ref('fct_encounter') }} where is_readmission_eligible
)