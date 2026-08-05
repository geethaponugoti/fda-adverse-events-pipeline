-- mart_drug_reactions.sql
-- Report volume by drug/reaction pair.

with stg as (

    select * from {{ ref('stg_fda_adverse_events') }}

),

aggregated as (

    select
        drug_name,
        reaction,
        count(*)                                as report_count,
        count(*) filter (where is_serious)      as serious_count,
        count(*) filter (where is_fatal)        as fatal_count,
        round(avg(patient_age), 1)              as avg_patient_age,
        max(received_date)                      as most_recent_report_date
    from stg
    group by
        drug_name,
        reaction

)

select * from aggregated
order by report_count desc
