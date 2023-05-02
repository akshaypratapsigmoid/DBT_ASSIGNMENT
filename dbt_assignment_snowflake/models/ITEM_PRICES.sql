{{
    config(
        materialized = 'incremental',
        schema = 'CONS',
        incremental_strategy = 'delete+insert',
    )
}}

with country_codes as (
    select country_key, country_name, load_time from dbt_db.stg.country_codes
),
item_prices as (
    select version, item_key, item_name, year, month, price, user, load_time from dbt_db.stg.item_prices_stg
)


select 
    version,
    country_key as country_code,
    country_name as country,
    item_key,
    item_name,
    year,
    month,
    price,
    user,
    CASE WHEN cc.load_time > ip.load_time
         THEN cc.load_time
         ELSE ip.load_time
    END as load_time
 from country_codes cc, item_prices ip
{% if is_incremental() %}
      where cc.load_time > (select max(load_time) from {{ this }})
    or ip.load_time > (select max(load_time) from {{this}})
{% endif %}
