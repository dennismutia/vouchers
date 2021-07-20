/******************************************************************************

Description:    Calculates the number of unique users per week
Returns:        A table containing the number of unique users per week

******************************************************************************/

{{
    config(
        materialized='incremental'
    )
}}

select 
    year,
    month_name as month,
    week_of_year,
    min(m.date) as start_of_week,
    max(m.date) as end_of_week,
count(distinct user_id) as no_of_users
from {{ ref('master_calendar') }} m
left join public.vouchers  v
    on m.date = v.date
group by year, month, week_of_year