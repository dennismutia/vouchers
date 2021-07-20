/******************************************************************************

Description:    Calculates number of redeemed vouchers per week
Returns:        A table containing the number of redeemed vouchers per week

******************************************************************************/

{{
    config(
        materialized='table'
    )
}}

select 
    year,
    month_name as month,
    week_of_year,
    min(m.date) as start_of_week,
    max(m.date) as end_of_week,
    v.status,
    count(distinct voucher_code) as no_of_redeemed_vouchers
from {{ ref('master_calendar') }} m
left join public.vouchers  v
    on m.date = v.date
group by year, month_name, week_of_year, v.status
having v.status = 'redeemed'