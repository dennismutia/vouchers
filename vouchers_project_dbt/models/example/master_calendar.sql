/*
Create a date timension with dates ranging from the minimum date in vouchers table to the maximum date in the same table.
This will enable the creation of a gapless grid to capture any date gaps in the vouchers dataset for computations.

input
-----
- min_date - the first date in the vouchers table from when the date dim should start
- max_date - the last date in the vouchers table to when the date dim should end.

output
-------
- date dim master_calendar with dates ranging from start of data to the last date of the dataset
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