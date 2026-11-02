select
    discharge_disposition,
    booland_agg(is_readmission_eligible) as is_readmission_eligible
from {{ ref('fct_encounter') }}
group by discharge_disposition