-- stg_fda_adverse_events.sql
-- Cleans and types raw openFDA drug adverse event reports.

with source as (
    select * from {{ source('raw', 'fda_adverse_events') }}
),

cleaned as (
    select
        trim(report_id)                                          as report_id,
        to_date(nullif(trim(received_date), ''), 'YYYYMMDD')      as received_date,

        case
            when trim(serious) = '1' then true
            when serious is null or trim(serious) = '' then null
            else false
        end                                                       as serious,

        case
            when trim(serious_death) = '1' then true
            when serious_death is null or trim(serious_death) = '' then null
            else false
        end                                                       as serious_death,

        case
            when trim(serious_hosp) = '1' then true
            when serious_hosp is null or trim(serious_hosp) = '' then null
            else false
        end                                                       as serious_hosp,

        case
            when trim(serious_life) = '1' then true
            when serious_life is null or trim(serious_life) = '' then null
            else false
        end                                                       as serious_life,

        cast(patient_age as numeric)                               as patient_age,
        trim(patient_age_unit)                                     as patient_age_unit,
        trim(patient_sex)                                          as patient_sex,

        upper(trim(drug_name))                                     as drug_name,
        trim(drug_indication)                                      as drug_indication,
        lower(trim(reaction))                                      as reaction,
        trim(outcome)                                              as outcome,
        trim(country)                                              as country,

        -- Explicit convenience flags (openFDA code: 1 = yes)
        case when trim(serious) = '1' then true else false end        as is_serious,
        case when trim(serious_death) = '1' then true else false end  as is_fatal,

        loaded_at,
        load_date

    from source
    where report_id is not null
      and trim(report_id) <> ''
)

select * from cleaned
