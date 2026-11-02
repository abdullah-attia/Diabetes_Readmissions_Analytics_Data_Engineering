with funnel as (
    select
        (select count(*) from {{ ref('fct_encounter') }})
          + (select count(*) from {{ ref('stg_rejected') }})        as raw_encounters,
        (select count(*) from {{ ref('stg_rejected') }})            as rejected_encounters,
        (select count(*) from {{ ref('fct_encounter') }})           as clean_encounters,
        (select count_if(not is_readmission_eligible)
           from {{ ref('fct_encounter') }})                         as excluded_death_hospice,
        (select count_if(is_readmission_eligible)
           from {{ ref('fct_encounter') }})                         as eligible_encounters
)

select
    r.rule_id,
    r.rule_name,
    r.rows_affected,
    r.action,
    r.notes,
    f.*
from {{ ref('stg_dq_summary') }} r
cross join funnel f