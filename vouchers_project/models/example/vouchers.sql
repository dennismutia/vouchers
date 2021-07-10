
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with vouchers as (

    select distinct 
        user_id,
        voucher_code,
        product,
        ltrim(split_part(vendor, ',', 1), '{') as country,
        btrim(rtrim(split_part(vendor, ',', 2), '}'), '"') as vendor,
        status,
        date
    from public.stg_vouchers
)

select *
from vouchers

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
