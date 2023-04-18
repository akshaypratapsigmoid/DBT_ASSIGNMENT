# DBT_Assignment
Github repository for DBT assignment

### 1) Load Country_code config using data provided
Output table: COUNTRY_CODES in schema STG
Country_code , Country_name

### Solution:
```
dbt seed
```
<img width="1440" alt="Screenshot 2023-04-10 at 11 05 59 PM" src="https://user-images.githubusercontent.com/122514456/230958434-04305495-87ea-44ba-b7c0-6334734e490a.png">

### Output:
<img width="1124" alt="Screenshot 2023-04-10 at 11 20 53 PM" src="https://user-images.githubusercontent.com/122514456/230963386-4548800f-7bfe-4002-a6f4-81cce8e0b04a.png">



### 2) Load File data to stg
Output table : ITEM_PRICES_STG in schema STG
Version, Country_code, Country, Item_key, Item_name, year, month, price, user

### Solution:
```
dbt seed
```
<img width="1440" alt="Screenshot 2023-04-10 at 11 06 14 PM" src="https://user-images.githubusercontent.com/122514456/230958488-3295896c-77d4-4606-ad6a-d18b5c03c3f9.png">

### Output:
<img width="1124" alt="Screenshot 2023-04-10 at 11 21 18 PM" src="https://user-images.githubusercontent.com/122514456/230963441-dd3e1710-bc88-4f8f-85f9-b65f29c9c443.png">



### 3) Create an incremental dbt model to load item prices to final prices based on file data loaded
Output table : ITEM_PRICES in schema CONS
Version, Country_code, Country, Item_key, Item_name, year, month, price, user

### Solution:
```
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
```
### Output:
<img width="1124" alt="Screenshot 2023-04-10 at 11 28 19 PM" src="https://user-images.githubusercontent.com/122514456/230963481-c9d57217-f2cc-41f1-9a8f-46f99ef9294b.png">
