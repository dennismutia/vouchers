
/*
This script creates a vouchers table with clean columns for analysis

input
--------
stg_vouchers table - staging table containing raw dataset from json files

output
--------
vouchers table - table with clean vouchers data

*/


{{
    config(
        materialized='table'
    )
}}


with products_summary as (
    select 
        year,
        month_name as month,
        week_of_year,
        min(m.date) as start_of_week,
        max(m.date) as end_of_week,
        product,
        count(distinct voucher_code) as no_of_vouchers
    from {{ ref('master_calendar') }} m
    left join public.vouchers  v
        on m.date = v.date
    group by year, month_name, week_of_year, product
),
products_ranking as (
    select 
        *,
        rank() over (partition by week_of_year order by no_of_vouchers desc) as product_rank
    from products_summary
)
select 
    * 
from products_ranking
where product_rank = 1


/*
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date::timestamp > (select max(date)::timestamp from vouchers)

{% endif %}
*/