/******************************************************************************

Description:    Calculates number of redeemed vouchers per week
Returns:        A table containing the number of redeemed vouchers per week

******************************************************************************/

{{
    config(
        materialized='table'
    )
}}


with redeemed_vouchers as (
    select 
        year,
        month_name as month,
        week_of_year,
        m.date,
        voucher_code
    from {{ ref('master_calendar') }} m
    left join public.vouchers  v
        on m.date = v.date
    where v.status = 'redeemed'
)
select 
    year,
    month,
    week_of_year,
    min(date) as start_of_week,
    max(date) as end_of_week,
    count(distinct voucher_code) as no_of_redeemed_vouchers
from redeemed_vouchers
group by year, month, week_of_year