# DBT_Assignment
Github repository for DBT assignment

### 1) Load Country_code config using data provided
Output table: COUNTRY_CODES in schema STG
Country_code , Country_name

### Solution:
```
dbt seed
```
<img width="1249" alt="Screenshot 2023-05-15 at 5 38 52 PM" src="https://github.com/akshaypratapsigmoid/DBT_ASSIGNMENT/assets/123646244/ca5a2834-7f67-45f4-888b-234751b0a3f8">

### Output:
<img width="1279" alt="Screenshot 2023-05-15 at 5 34 55 PM" src="https://github.com/akshaypratapsigmoid/DBT_ASSIGNMENT/assets/123646244/92cd7241-e57e-42b2-bafb-c04ea8a62108">


### 2) Load File data to stg
Output table : ITEM_PRICES_STG in schema STG
Version, Country_code, Country, Item_key, Item_name, year, month, price, user

### Solution:
```
dbt seed
```
<img width="1249" alt="Screenshot 2023-05-15 at 5 38 58 PM" src="https://github.com/akshaypratapsigmoid/DBT_ASSIGNMENT/assets/123646244/e2e55387-a231-4840-97cc-d93186489a24">

### Output:
<img width="1279" alt="Screenshot 2023-05-15 at 5 36 07 PM" src="https://github.com/akshaypratapsigmoid/DBT_ASSIGNMENT/assets/123646244/b643ef32-2a8a-47e0-aae2-1d3a55c2d6c8">


### 3) Create an incremental dbt model to load item prices to final prices based on file data loaded
Output table : ITEM_PRICES in schema CONS
Version, Country_code, Country, Item_key, Item_name, year, month, price, user

### Solution:
```
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

```
### Output:
<img width="1124" alt="Screenshot 2023-04-10 at 11 28 19 PM" src="https://user-images.githubusercontent.com/122514456/230963481-c9d57217-f2cc-41f1-9a8f-46f99ef9294b.png">
