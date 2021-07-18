
/*
This script creates a vouchers table with clean columns for analysis

input
--------
stg_vouchers table - staging table containing raw dataset from json files

output
--------
vouchers table - table with clean vouchers data

*/

/*
{{
    config(
        materialized='incremental'
    )
}}
*/

with users as (
    select 
        year,
        month_name as month,
        week_of_year,
        m.date,
        user_id
    from {{ ref('master_calendar') }} m
    left join public.vouchers  v
        on m.date = v.date
)
select 
    year,
    month,
    week_of_year,
    min(date) as start_of_week,
    max(date) as end_of_week,
    count(distinct user_id) as no_of_users
from users
group by year, month, week_of_year

/*
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date::timestamp > (select max(date)::timestamp from vouchers)

{% endif %}
*/