
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
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