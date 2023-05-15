# Peer_Learning_Document

# Karan's Approach 

This is my DBT Assignment
#### Below I have explain the approach,defined the steps in complete detail for the questions needed and shown the output screenshot to my solution:

### Before starting with Questions:
* Firstly before starting the project we initialized the folder with ```dbt init dbt_assignment``` command where __dbt_assignment__ is the project name in   my case
* Basically ```dbt init``` helps to set up a ```connection profile``` so that one can start working quickly.
* It __prompts__ for each piece of information that __dbt needs__ to connect to that database: things like ```account```, ```user```, ```password```, etc
* Eventually, it will __create__ a __folder__ with the necessary ```files and folders``` to get ```started```
* Here Karan have used Snowflake database and provided the credential for the same while specifying information in dbt init.
* All the details regarding the connection are stored in the default location is ```~/.dbt/profiles.yml```.

## Question 1 / Question 2 :
#### Load ```Country_code``` config using data provided
* Output table: ```COUNTRY_CODES``` in schema ```STG```
* ```Country_code``` , ```Country_name```
* Hint:use dbt seed

#### Load ```ITEM_PRICES_STG``` data to stg
* Output table : ```ITEM_PRICES_STG``` in schema ```STG```
* ```Version```, ```Country```, ```Item_key```,```Item_name```,```year```,```month```,```price```,```user```
* Hint:use dbt seed


### Solution:-
### Steps:
#### First Uploaded the CSV files to the seeds folder 
<img width="281" alt="Screenshot 2023-04-10 at 10 38 12 PM" src="https://user-images.githubusercontent.com/122456892/230953352-de7abdbc-9819-4ca9-b0be-75ed0b8389a1.png">

#### After placing CSV files in the seeds folder ran ```dbt seed``` command
<img width="954" alt="Screenshot 2023-04-10 at 10 47 12 PM" src="https://user-images.githubusercontent.com/122456892/230956263-a50c3ff8-0f04-49f8-9911-a71853993643.png">

### Note:
__Seeds__ are CSV files in your ```dbt project``` __(typically in your seeds directory)__, that dbt can load into your __data warehouse__ using the ```dbt seed``` command.

### Explanation:
* First placed both the CSV files into seeds folder
* Then after ran ```dbt seed``` to upload the file in the ```target schema``` i.e., ```STG``` in snowflake. 
* Defined the target schema as ```STG```  while creating the connection with snowflake.

### Verification for files in STG stage:
<img width="315" alt="Screenshot 2023-04-11 at 12 09 08 AM" src="https://user-images.githubusercontent.com/122456892/230970165-3004345b-53fd-42df-ba21-a297a5fd546d.png">

### Querying ```Country_Codes``` data:
<img width="831" alt="Screenshot 2023-04-11 at 12 12 58 AM" src="https://user-images.githubusercontent.com/122456892/230970834-2c385a0c-6056-4fb9-8134-950fcfad3191.png">

### Querying ```Item_prices_stg``` data:
<img width="860" alt="Screenshot 2023-04-11 at 12 15 44 AM" src="https://user-images.githubusercontent.com/122456892/230971231-83381f30-dbe6-49c0-8e50-898de7dc9128.png">

## Question 3:
#### Create an ```incremental``` dbt model to load ```item prices``` to ```final prices``` based on file data loaded
* Output table : ```ITEM_PRICES``` in in schema ```CONS```
* ```Version```, ```Country_code``` , ```Country```, ```Item_key```, ```Item_name```, ```year```,```month```, ```price```,```user```
* Hints:

    1.) Generate data for each country based on country code and global data
    
    2.)Use incremental strategy to load only new data
    
### Solution:-

#### Firstly created the ```cons folder``` inside ```model folder``` and then after created the ```Item_prices.sql``` file inside ```cons``` folder.

```
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key =['Version','Country_Code','Item_key','Year','Month','price','User']
) }}

with seed_country_codes as
(
    SELECT * FROM {{ ref('Country_codes') }}
),
seed_item_prices_stg as(
    SELECT * FROM {{ ref('Item_prices_stg') }}
)
SELECT 
ip.Version,
cc.country_key as country_code,
cc.country_name as Country,
ip.Item_Key,
ip.Item_name,
ip.Year,
ip.Month,
ip.price,
ip.User
FROM seed_country_codes as cc, seed_item_prices_stg as ip

```

#### Explanation:
* Here After __creating__ the file, I have specified the ```sql script``` wherein I am using Two __CTEs__ to extract the data from both the csv files in     the ```STG schema``` and then extracted the required columns from both the tables and changed the __attributes__ name as per the expected output file       provided.
* Above Karan is using ```jinja template``` for refering to the required file using ```ref()``` function.
* Here we have been asked to use ```incremental dbt model``` so in the ```config``` Karan have specified the __materialized__ as ```incremental``` and Karan have   used the incremental strategy as ```merge``` and specified the unique keys which will be used by the ```merge``` to check for unique data, if data is     unique it will insert the data otherwise it will update the existing data.
* If unique key already exists in the destination table, merge will update the record, so you will not have duplicates. And if the records don’t exist       merge will insert them.

#### For creating the ```CONS``` stage I have included the stage name in ```dbt_project.yml``` file under ```cons``` folder

<img width="860" alt="Screenshot 2023-04-11 at 12 56 01 AM" src="https://user-images.githubusercontent.com/122456892/230980719-90016173-4255-4a41-9215-855d2298c101.png">

### Running ```dbt run``` command

<img width="946" alt="Screenshot 2023-04-11 at 12 59 32 AM" src="https://user-images.githubusercontent.com/122456892/230981497-ee481910-e256-4f0a-98fb-64621042b681.png">

### Verification for files in ```CONS``` stage:
<img width="286" alt="Screenshot 2023-04-11 at 1 06 17 AM" src="https://user-images.githubusercontent.com/122456892/230982668-b566bea0-b4ad-411f-ab7e-2c4bc95f836d.png">

### Querying ```Item_prices``` data:

<img width="1022" alt="Screenshot 2023-04-11 at 1 04 39 AM" src="https://user-images.githubusercontent.com/122456892/230982453-effa501b-0aa6-4589-91ba-c3e723571685.png">


# Sarthak's Approach

# 1) Load Country_code config using data provided
Output table: COUNTRY_CODES in schema STG Country_code , Country_name

### Solution:
```
dbt seed 
```
<img width="942" alt="Screenshot 2023-04-13 at 5 18 55 PM" src="https://user-images.githubusercontent.com/123497764/231749676-db640912-dea3-49ac-9354-5eec8b65a2cb.png">


<img width="842" alt="Screenshot 2023-04-13 at 5 17 08 PM" src="https://user-images.githubusercontent.com/123497764/231749215-ea0de14d-7483-4ce7-96e5-889b4b4c42a0.png">

# 2) Load File data to stg
Output table : ITEM_PRICES_STG in schema STG Version, Country_code, Country, Item_key, Item_name, year, month, price, user

### Solution:
```
dbt seed
```
<img width="1060" alt="Screenshot 2023-04-13 at 5 40 00 PM" src="https://user-images.githubusercontent.com/123497764/231754066-5285f6a0-cd8a-4694-828a-1afd03e84c09.png">


<img width="1104" alt="Screenshot 2023-04-13 at 5 41 13 PM" src="https://user-images.githubusercontent.com/123497764/231754351-14b4b923-a6f5-46a0-babd-139e3592b0cf.png">

# 3) Create an incremental dbt model to load item prices to final prices based on file data loaded
Output table : ITEM_PRICES in schema CONS Version, Country_code, Country, Item_key, Item_name, year, month, price, user

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
<img width="1111" alt="Screenshot 2023-04-13 at 5 45 57 PM" src="https://user-images.githubusercontent.com/123497764/231755392-4e6a2341-fab6-4eb3-9370-93774969c097.png">
