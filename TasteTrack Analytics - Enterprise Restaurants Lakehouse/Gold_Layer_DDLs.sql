

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