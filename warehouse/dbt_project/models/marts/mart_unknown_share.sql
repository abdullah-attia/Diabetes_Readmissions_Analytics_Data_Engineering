with e as (
    select * from {{ ref('fct_encounter') }}
)

select 'Race' as field, count_if(coalesce(race, 'Unknown') = 'Unknown') / count(*) as unknown_share from e
union all
select 'Payer', count_if(payer = 'Unknown') / count(*) from e
union all
select 'Medical specialty', count_if(medical_specialty = 'Unknown') / count(*) from e
union all
select 'Admission type', count_if(admission_type = 'Unknown') / count(*) from e
union all
select 'Admission source', count_if(admission_source = 'Unknown') / count(*) from e
union all
select 'Discharge destination', count_if(discharge_disposition = 'Unknown') / count(*) from e
union all
select 'Primary diagnosis', count_if(primary_diagnosis_group = 'Unknown') / count(*) from e