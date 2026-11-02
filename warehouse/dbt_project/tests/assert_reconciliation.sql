select 1
where (select count(*) from {{ ref('fct_encounter') }})
    + (select count(*) from {{ ref('stg_rejected') }}) <> 101766