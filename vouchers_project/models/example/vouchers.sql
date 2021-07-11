
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
        materialized='incremental'
    )
}}

select distinct 
    user_id,
    voucher_code,
    product,
    ltrim(split_part(vendor, ',', 1), '{') as country,
    btrim(rtrim(split_part(vendor, ',', 2), '}'), '"') as vendor,
    status,
    date::timestamp as date
from public.stg_vouchers

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date::timestamp > (select max(date)::timestamp from vouchers)

{% endif %}