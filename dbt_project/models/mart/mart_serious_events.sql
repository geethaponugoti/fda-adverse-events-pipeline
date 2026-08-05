-- mart_serious_events.sql
-- Serious-only report volume by drug/outcome, ranked within each drug.
-- Only drug/outcome pairs with more than 5 reports are surfaced.

with stg as (

    select * from {{ ref('stg_fda_adverse_events') }}
    where is_serious

),

aggregated as (

    select
        drug_name,
        outcome,
        count(*)                          as report_count,
        count(*) filter (where is_fatal)  as fatal_count,
        round(avg(patient_age), 1)        as avg_patient_age,
        max(received_date)                as most_recent_date
    from stg
    group by
        drug_name,
        outcome

),

ranked as (

    select
        *,
        rank() over (
            partition by drug_name
            order by report_count desc
        ) as severity_rank
    from aggregated

)

select * from ranked
where report_count > 5
order by drug_name, severity_rank
