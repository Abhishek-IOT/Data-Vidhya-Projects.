

create or replace table GOLD_LAYER.DIM_ORDERS (
order_key string,
order_id string,
ORDER_PLACED_AT string,
REVIEW string,
EMPLOYEE_1 string,
EMPLOYEE_2 string,
DELIVERY_PARTNER string,
    created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string
);



create or replace table GOLD_LAYER.dim_restaurants (
restaurant_key string,
restaurant_id string,
    Restaurant_Name string,
    Subzone string,
    City string,
    created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string

);



create catalog ADF;

create schema gold_layer;
create schema ADF.silver_layer;

drop schema silver_layer;

SELECT current_catalog(), current_schema();

use schema silver;


with dim_restuarants as 
(

    select 
    md5(concat(Restaurant_Name,Subzone,City)) as restaurant_key,
    Restaurant_Name,Subzone,city from silver.restaurant_orders 
)
select * from dim_restuarants 
;


 select * from silver.restaurant_orders limit 5;

 show tables in workspace.silver;

 CREATE or replace TABLE adf.silver_layer.menu
AS
SELECT *
FROM workspace.Silver.menu;

select * from adf.silver_layer.menu;


select * from adf.gold_layer.dim_restaurants limit 5;


select * from silver.restaurant_orders limit 5;

with orders as (
select 
order_id,
ORDER_PLACED_AT,
REVIEW,
'N' as online_channel,
Waiter_ID as EMPLOYEE_1,
Chef_ID as EMPLOYEE_2,
'N/A' as DELIVERY_PARTNER
 from
silver.restaurant_orders
union all
select 
 order_id,
ORDER_PLACED_AT,
REVIEW,
'Y' as online_channel,
'N/A' as EMPLOYEE_1,
'EMP005' as EMPLOYEE_2,
DELIVERY_PARTNER 
from 
silver.restaurant_orders_online)
select *,
row_number() over (partition by order_id order by order_id) as order_key
 from orders
;


truncate table adf.gold_layer.dim_restaurants;
select * from adf.gold_layer.dim_restaurants limit 5;
MERGE INTO adf.gold_layer.dim_restaurants as A using
(
  select 
    md5(concat(Restaurant_Name,Subzone,City)) as restaurant_key,
    Restaurant_Name,
    Subzone,
    current_timestamp() as created_at,
    'DBT' as created_user,
    current_timestamp() as updated_at,
    'DBT' as updated_user,
    city from silver.restaurant_orders 
)
as B on b.restaurant_key=a.restaurant_key
when matched then update
set 
a.Restaurant_Name=b.Restaurant_Name,
a.Subzone=b.Subzone,
a.city=b.city,
a.updated_at=current_timestamp()

when not matched then insert
(
   restaurant_key,
Restaurant_Name,
    Subzone,
    city,
    created_at,
    created_user,
    updated_at,
    updated_user
)
values 
(
 b.restaurant_key,
b.Restaurant_Name,
b.Subzone,
b.city,
b.created_at,
b.created_user,
b.updated_at,
b.updated_user
);





select *,md5(string(order_id)) as order_key
from
(select 
order_id,
ORDER_PLACED_AT,
REVIEW,
'N' as online_channel,
Waiter_ID as EMPLOYEE_1,
Chef_ID as EMPLOYEE_2,
'N/A' as DELIVERY_PARTNER
 from
silver_layer.restaurant_orders
union all
select 
 order_id,
ORDER_PLACED_AT,
REVIEW,
'Y' as online_channel,
'N/A' as EMPLOYEE_1,
'EMP005' as EMPLOYEE_2,
DELIVERY_PARTNER 
from 
silver_layer.restaurant_orders_online
);





create or replace table GOLD_LAYER.DIM_ORDERS (
order_key string,
order_id string,
ORDER_PLACED_AT string,
REVIEW string,
EMPLOYEE_1 string,
EMPLOYEE_2 string,
DELIVERY_PARTNER string,
    created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string
);


select* from GOLD_LAYER.DIM_ORDERS;

truncate GOLD_LAYER.DIM_ORDERS;
MERGE INTO GOLD_LAYER.DIM_ORDERS AS A USING
(select *,md5(string(order_id)) as order_key,
        current_timestamp() AS created_at,
        'DBT' AS created_user,
        current_timestamp() AS updated_at,
        'DBT' AS updated_user
from
(select 
order_id,
ORDER_PLACED_AT,
REVIEW,
'N' as online_channel,
Waiter_ID as EMPLOYEE_1,
Chef_ID as EMPLOYEE_2,
'N/A' as DELIVERY_PARTNER
 from
silver_layer.restaurant_orders
union all
select 
 order_id,
ORDER_PLACED_AT,
REVIEW,
'Y' as online_channel,
'N/A' as EMPLOYEE_1,
'EMP005' as EMPLOYEE_2,
DELIVERY_PARTNER 
from 
silver_layer.restaurant_orders_online
)
)
AS B ON A.order_key=B.order_key

WHEN MATCHED THEN UPDATE 
SET 
A.order_id=B.order_id,
A.ORDER_PLACED_AT=B.ORDER_PLACED_AT,
A.REVIEW=B.REVIEW,
A.EMPLOYEE_1=B.EMPLOYEE_1,
A.EMPLOYEE_2=B.EMPLOYEE_2,
A.DELIVERY_PARTNER=B.DELIVERY_PARTNER,
  A.updated_at = current_timestamp()

WHEN NOT MATCHED THEN INSERT
(
order_key,
order_id,
ORDER_PLACED_AT,
REVIEW,
EMPLOYEE_1,
EMPLOYEE_2,
DELIVERY_PARTNER,
    created_at,
    created_user,
    updated_at,
    updated_user
)
VALUES
(
    b.order_key,
b.order_id,
b.ORDER_PLACED_AT,
b.REVIEW,
b.EMPLOYEE_1,
b.EMPLOYEE_2,
b.DELIVERY_PARTNER,
b.created_at,
b.created_user,
b.updated_at,
b.updated_user
)
;

select current_timestamp();

create or replace table gold_layer.dim_employee(
    EMPLOYEE_KEY string,
EMPLOYEE_NAME string,
SALARY decimal,
BAND string,
MANAGER string,
RATING decimal,
DESIGNATION string,
is_active string,
effective_from timestamp,
effective_to timestamp,
hashdiff string,
createdat timestamp,
created_user timestamp
);



    select md5(EMPLOYEE_NAME) as EMPLOYEE_KEY,
    EMPLOYEE_NAME,
    Monthly_Salary AS SALARY,
    Salary_Band AS BAND,
    Manager_Name AS MANAGER,
    RATING,
    DESIGNATION,
    'Y' as is_active,
    current_timestamp() as effective_from,
    '2050-07-26T12:10:41.480+00:00' as effective_to,
    md5(concat(EMPLOYEE_NAME,string(SALARY),band,manager,rating,DESIGNATION)) as hashdiff
    from silver_layer.employee;

    select * from silver_layer.employee limit 4;

 update silver_layer.employee set
 Rating='5' where Employee_Name='Priya Kapoor';   

truncate table GOLD_LAYER.dim_employee;
select * from GOLD_LAYER.dim_employee;

MERGE INTO GOLD_LAYER.dim_employee as a using 
(
 select md5(EMPLOYEE_NAME) as EMPLOYEE_KEY,
    EMPLOYEE_NAME,
    Monthly_Salary AS SALARY,
    Salary_Band AS BAND,
    Manager_Name AS MANAGER,
    RATING,
    DESIGNATION,
    'Y' as is_active,
    current_timestamp() as effective_from,
    '2050-07-26T12:10:41.480+00:00' as effective_to,
    md5(concat(EMPLOYEE_NAME,string(SALARY),band,manager,rating,DESIGNATION)) as hashdiff
    from silver_layer.employee
)
as b on a.EMPLOYEE_KEY=b.EMPLOYEE_KEY and a.hashdiff<>b.hashdiff

when matched then update 
set 
a.EMPLOYEE_NAME=b.EMPLOYEE_NAME,
a.SALARY=b.SALARY,
a.BAND=b.band,
a.MANAGER=b.manager,
a.RATING=b.RATING,
a.DESIGNATION=b.DESIGNATION,
is_active='N',
 effective_to=current_timestamp()

when not matched then INSERT
(
EMPLOYEE_KEY,
EMPLOYEE_NAME,
SALARY,
BAND,
MANAGER,
RATING,
DESIGNATION,
is_active,
effective_from,
effective_to,
hashdiff
)
VALUES
(
b.EMPLOYEE_KEY,
b.EMPLOYEE_NAME,
b.SALARY,
b.BAND,
b.MANAGER,
b.RATING,
b.DESIGNATION,
b.is_active,
b.effective_from,
b.effective_to,
b.hashdiff
);




create or replace table gold_layer.dim_menu 
(
ITEM_KEY string,
ITEM_NAME string,
ITEM_TYPE string,
PRICE decimal,
is_active string,
effective_from timestamp,
effective_to timestamp,
created_at timestamp,
created_user string,
hashdiff string
);

truncate table gold_layer.dim_menu;
select * from gold_layer.dim_menu;

insert into gold_layer.dim_menu
select 
md5(ITEM_NAME) as item_key,
ITEM_NAME,
ITEM_TYPE,
PRICE,
 'Y' as is_active,
    current_timestamp() as effective_from,
    '2050-07-26T12:10:41.480+00:00' as effective_to,
 current_timestamp() as created_at,
 'DBT' as created_user,
md5(concat(ITEM_NAME,ITEM_TYPE,string(price))) as hashdiff
    
 from silver_layer.MENU;

 truncate table silver_layer.MENU;


select * from   silver_layer.MENU limit 5;

delete from silver_layer.MENU
where price='780';


create table gold_layer.fact_sales
(
SALES_KEY string,
RESTRAUNT_KEY string,
ORDER_KEY string,
EMPLOYEE_KEY string,
ITEM_KEY string,
BILL_SUBTOTAL decimal,
PACKAGING_CHARGES decimal,
RESTAURANT_DISCOUNT_PROMO decimal,
RESTAURANT_DISCOUNT_FLAT_OFF decimal,
GOLD_DISCOUNT decimal,
BRAND_PACK_DISCOUNT decimal,
TOTAL_AMOUNT decimal,
DISTANCE_KM decimal,
 created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string
);
select * from silver_layer.order_details;

CREATE OR REPlace table silver_layer.order_details as 
select 
RESTAURANT_ID,
ORDER_ID,
Waiter_ID,
Chef_ID
 from silver_layer.restaurant_orders
union all
select RESTAURANT_ID,
ORDER_ID,
'N/a' as waiter_id,
'N' as chef_id
from 
silver_layer.restaurant_orders_online;


create or replace table GOLD_LAYER.dim_restaurants (
restaurant_key string,
restaurant_id string,
    Restaurant_Name string,
    Subzone string,
    City string,
    created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string

);


select * from  silver_layer.employee limit 5;


create or replace table gold_layer.fact_sales
(
SALES_KEY string,
RESTRAUNT_KEY string,
ORDER_KEY string,
EMPLOYEE_KEY_CHEF string,
EMPLOYEE_KEY_WAITER string,
EMPLOYEE_KEY_MANAGER string,
ITEM_KEY string,
BILL_SUBTOTAL decimal,
PACKAGING_CHARGES decimal,
RESTAURANT_DISCOUNT_PROMO decimal,
RESTAURANT_DISCOUNT_FLAT_OFF decimal,
GOLD_DISCOUNT decimal,
BRAND_PACK_DISCOUNT decimal,
TOTAL_AMOUNT decimal,
DISTANCE_KM decimal,
 created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string
);



-- MERGE INTO gold_layer.fact_sales a
-- USING (
SELECT 
MD5(CONCAT(dr.restaurant_key,ddo.order_key)) AS SALES_KEY,
--ORDER_KEY,EMPLOYEE_KEY_CHEF,EMPLOYEE_KEY_WAITER,EMPLOYEE_KEY_MANAGER
dr.restaurant_key as RESTRAUNT_KEY,
ddo.order_key,
de1.EMPLOYEE_KEY
from silver_layer.order_details base
left join GOLD_LAYER.dim_restaurants dr on
base.RESTAURANT_ID=dr.RESTAURANT_ID
left join GOLD_Layer.dim_orders ddo on 
base.ORDER_ID=ddo.order_id
left join  gold_layer.dim_employee de1 on 
base.Chef_ID=de1.employee_id
;

MERGE INTO GOLD_LAYER.dim_employee as a using 
(
 select md5(EMPLOYEE_NAME) as EMPLOYEE_KEY,
 EMPLOYEE_ID,
    EMPLOYEE_NAME,
    Monthly_Salary AS SALARY,
    Salary_Band AS BAND,
    Manager_Name AS MANAGER,
    RATING,
    DESIGNATION,
    'Y' as is_active,
    current_timestamp() as effective_from,
    '2050-07-26T12:10:41.480+00:00' as effective_to,
    md5(concat(EMPLOYEE_NAME,string(SALARY),band,manager,rating,DESIGNATION)) as hashdiff,
    current_timestamp as created_at,
    'DBT' AS created_user
    from silver_layer.employee
)
as b on a.EMPLOYEE_KEY=b.EMPLOYEE_KEY and a.hashdiff<>b.hashdiff

when matched then update 
set 
a.EMPLOYEE_ID=b.EMPLOYEE_ID,
a.EMPLOYEE_NAME=b.EMPLOYEE_NAME,
a.SALARY=b.SALARY,
a.BAND=b.band,
a.MANAGER=b.manager,
a.RATING=b.RATING,
a.DESIGNATION=b.DESIGNATION,
is_active='N',
 effective_to=current_timestamp()

when not matched then INSERT
(
EMPLOYEE_KEY,
EMPLOYEE_ID,
EMPLOYEE_NAME,
SALARY,
BAND,
MANAGER,
RATING,
DESIGNATION,
is_active,
effective_from,
effective_to,
hashdiff,
created_at,
created_user
)
VALUES
(
b.EMPLOYEE_KEY,
b.EMPLOYEE_ID,
b.EMPLOYEE_NAME,
b.SALARY,
b.BAND,
b.MANAGER,
b.RATING,
b.DESIGNATION,
b.is_active,
b.effective_from,
b.effective_to,
b.hashdiff,
b.created_at,
b.created_user
)
;



create or replace table gold_layer.dim_employee(
    EMPLOYEE_KEY string,
    employee_id string,
EMPLOYEE_NAME string,
SALARY decimal,
BAND string,
MANAGER string,
RATING decimal,
DESIGNATION string,
is_active string,
effective_from timestamp,
effective_to timestamp,
created_at timestamp,
created_user string,
hashdiff string
);



create or replace table gold_layer.dim_menu 
(
ITEM_KEY string,
ITEM_NAME string,
ITEM_TYPE string,
PRICE decimal,
is_active string,
effective_from timestamp,
effective_to timestamp,
createdat timestamp,
created_user timestamp
);


-- For Loading Fact Table.
CREATE OR REPlace table silver_layer.order_details as 
select 
RESTAURANT_ID,
ORDER_ID,
Waiter_ID,
Chef_ID,
ITEM_NAME,
BILL_SUBTOTAL,
0 as PACKAGING_CHARGES,
RESTAURANT_DISCOUNT_PROMO,
RESTAURANT_DISCOUNT_FLAT_OFF,
GOLD_DISCOUNT,
BRAND_PACK_DISCOUNT,
TOTAL_AMOUNT,
0 as DISTANCE_KM
 from silver_layer.restaurant_orders
union all
select RESTAURANT_ID,
ORDER_ID,
'N/a' as waiter_id,
'N' as chef_id,
ITEM_NAME,
BILL_SUBTOTAL,
PACKAGING_CHARGES,
RESTAURANT_DISCOUNT_PROMO,
RESTAURANT_DISCOUNT_FLAT_OFF,
GOLD_DISCOUNT,
BRAND_PACK_DISCOUNT,
TOTAL_AMOUNT,
DISTANCE_KM
from 
silver_layer.restaurant_orders_online;


create table gold_layer.fact_sales
(
SALES_KEY string,
RESTRAUNT_KEY string,
ORDER_KEY string,
EMPLOYEE_KEY_CHEF string,
EMPLOYEE_KEY_WAITER string,
ITEM_KEY string,
BILL_SUBTOTAL decimal,
PACKAGING_CHARGES decimal,
RESTAURANT_DISCOUNT_PROMO decimal,
RESTAURANT_DISCOUNT_FLAT_OFF decimal,
GOLD_DISCOUNT decimal,
BRAND_PACK_DISCOUNT decimal,
TOTAL_AMOUNT decimal,
DISTANCE_KM decimal,
 created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string
);