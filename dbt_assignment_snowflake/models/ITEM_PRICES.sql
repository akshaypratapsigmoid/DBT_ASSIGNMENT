{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['version', 'country_code', 'item_key', 'year', 'month', 'price', 'user'],
    schema = 'CONS'
    )
}}

with item_prices as (
select 
ips.version, cc.country_code, cc.country_name, ips.item_key, ips.item_name, ips.year, ips.month, ips.price, ips.user
from country_codes as cc
cross join item_prices_stg as ips
)
select * from item_prices