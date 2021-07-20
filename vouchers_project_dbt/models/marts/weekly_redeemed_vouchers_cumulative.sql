/******************************************************************************

Description:    Calculates the cumulative number of redemed vouchers per week
Returns:        A table containing the cumulative number of redeemed vouchers every week

******************************************************************************/

{{
    config(
        materialized='table'
    )
}}

with weekly_redemptions as (
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
)
select
    *,
    sum(no_of_redeemed_vouchers) over( order by week_of_year ) as no_of_redeemed_vouchers_cumulative
from weekly_redemptions