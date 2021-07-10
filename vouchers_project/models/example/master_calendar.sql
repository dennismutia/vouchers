/*
Create a date timension with dates ranging from the minimum date in vouchers table to the maximum date in the same table.
*/

{{
    config(
        materialized='incremental'
    )
}}

with range_values as (
    select
        min(date) as min_date,
        max(date) as max_date
    from {{ ref('vouchers') }} 
),

date_range as (
    select 
        generate_series(min_date::timestamp, max_date::timestamp, '1 day'::interval) as date
    from range_values
)

select
    date,
    EXTRACT(ISODOW FROM date) AS day_of_week,
    EXTRACT(DAY FROM date) AS day_of_month,
    TO_CHAR(date, 'W')::INT AS week_of_month,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(MONTH FROM date) AS month,
    TO_CHAR(date, 'Month') AS month_name,
    EXTRACT(YEAR FROM date) AS year
from date_range

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date > (select max(date)::timestamp from master_calendar)

{% endif %}