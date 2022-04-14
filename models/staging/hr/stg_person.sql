/* Example to show incremental loading with pre and post hook configuration 
*/
{{ config(
    pre_hook=[
      "truncate table {{ this }} "
    ]
) }}

{{
    config(
        materialized='incremental'
    )
}}

with person as (
    select id as person_id,
    gender,
    job_id,
    case when job_id <15 then 'Level 1'
         when job_id >=15 and job_id <20 then 'Level 2'
         when job_id >=20 and job_id <25 then 'Level 3'
         when job_id >=25 and job_id <30 then 'Level 4'
    else
         'Uncategorised'
    end job_classification,
    initcap(first_names)||' '||initcap(last_name) full_name,
    ni_number,
    date_of_birth,
    case when extract('month' from date_of_birth::timestamp)>extract('month' from getdate()::timestamp) then datediff('week',date_of_birth::timestamp,current_timestamp::timestamp)/52
       when extract('month' from date_of_birth::timestamp)>=extract('month' from getdate()::timestamp) and extract('day' from date_of_birth::timestamp)>=extract('day' from getdate()::timestamp) then (datediff('week',date_of_birth::timestamp,current_timestamp::timestamp)/52)-1
       when extract('month' from date_of_birth::timestamp)>=extract('month' from getdate()::timestamp) and extract('day' from date_of_birth::timestamp)<extract('day' from getdate()::timestamp) then (datediff('week',date_of_birth::timestamp,current_timestamp::timestamp)/52)
       else (datediff('week',date_of_birth::timestamp,current_timestamp::timestamp)/52)
    end approx_age_at_update,
    case when marital_status ='S' then 'Single'
         when marital_status ='M' then 'Married'
         when marital_status ='W' then 'Widowed'
    else
         'Unspecified'
    end marital_status,
    start_date,
    last_update_date,
    status_review_date,
    leave_date from {{ source('hr', 'person') }}
    {% if is_incremental() %}
      where last_update_date::timestamp > (select fetch_timestamp::timestamp from dbreeze.incr_control where model_name='stg_person')
    {% endif %}
 )

 select * from person

 {{ config(
    post_hook=[
      "update dbreeze.incr_control set fetch_timestamp=(select max(last_update_date)::timestamp from {{ this }} where model_name='stg_person') "
    ]
) }}