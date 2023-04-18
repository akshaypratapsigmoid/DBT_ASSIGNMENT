{{
  config(
    materialized = 'incremental',
    schema = 'CONS'
    )
}}  

with Country_code as (
select * from DBT.STG.COUNTRY_CODES
) ,
Global_Item_Prices as (
    select * from DBT.STG.GLOBAL_ITEM_PRICES
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
case when cc.load_time > ip.load_time then
cc.load_time
else
ip.load_time end
as load_time
from Country_code cc , Global_Item_Prices ip
{% if is_incremental() %}
where cc.load_time > (select max(load_time) from {{ this }})
or ip.load_time > (select max(load_time) from {{ this }})
{% endif %}
