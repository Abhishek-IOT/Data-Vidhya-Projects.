{% macro fact_sales_load() %}

{% set sql %}



MERGE INTO gold_layer.fact_sales a
USING (
SELECT 
MD5(CONCAT(dr.restaurant_key,ddo.order_key)) AS SALES_KEY,
dr.restaurant_key as RESTRAUNT_KEY,
ddo.order_key as ORDER_key,
de1.EMPLOYEE_KEY as EMPLOYEE_KEY_CHEF,
de2.EMPLOYEE_KEY as EMPLOYEE_KEY_WAITER,
dm.ITEM_KEY as item_key,
base.BILL_SUBTOTAL as bill_subtotal,
base.PACKAGING_CHARGES as PACKAGING_CHARGES,
base.RESTAURANT_DISCOUNT_PROMO as RESTAURANT_DISCOUNT_PROMO,
base.RESTAURANT_DISCOUNT_FLAT_OFF as RESTAURANT_DISCOUNT_FLAT_OFF,
base.GOLD_DISCOUNT as GOLD_DISCOUNT,
base.BRAND_PACK_DISCOUNT as BRAND_PACK_DISCOUNT,
base.TOTAL_AMOUNT as TOTAL_AMOUNT,
base.DISTANCE_KM as DISTANCE_KM,
current_timestamp() as created_at,
'BDR' as created_user,
current_timestamp() as updated_at,
'BDR' as updated_user
from silver_layer.order_details base
left join GOLD_LAYER.dim_restaurants dr on
base.RESTAURANT_ID=dr.RESTAURANT_ID
left join GOLD_Layer.dim_orders ddo on 
base.ORDER_ID=ddo.order_id
left join  gold_layer.dim_employee de1 on 
base.Chef_ID=de1.employee_id
left join  gold_layer.dim_employee de2 on 
base.waiter_id=de2.employee_id
left join gold_layer.dim_menu dm on 
base.item_name=dm.ITEM_NAME)
as b on 
a.sales_key=b.sales_key

when MATCHED then update SET
a.RESTRAUNT_KEY=b.RESTRAUNT_KEY,
a.order_key=b.order_key,
a.EMPLOYEE_KEY_CHEF=b.EMPLOYEE_KEY_CHEF,
a.EMPLOYEE_KEY_WAITER=b.EMPLOYEE_KEY_WAITER,
a.item_key=b.item_key,
a.bill_subtotal=b.bill_subtotal,
a.PACKAGING_CHARGES=b.PACKAGING_CHARGES,
a.RESTAURANT_DISCOUNT_PROMO=b.RESTAURANT_DISCOUNT_PROMO,
a.RESTAURANT_DISCOUNT_FLAT_OFF=b.RESTAURANT_DISCOUNT_FLAT_OFF,
a.GOLD_DISCOUNT=b.GOLD_DISCOUNT,
a.BRAND_PACK_DISCOUNT=b.BRAND_PACK_DISCOUNT,
a.TOTAL_AMOUNT=b.TOTAL_AMOUNT,
a.DISTANCE_KM=b.DISTANCE_KM


when not matched then insert 
(
SALES_KEY ,
RESTRAUNT_KEY ,
ORDER_KEY ,
EMPLOYEE_KEY_CHEF ,
EMPLOYEE_KEY_WAITER ,
ITEM_KEY ,
BILL_SUBTOTAL ,
PACKAGING_CHARGES ,
RESTAURANT_DISCOUNT_PROMO ,
RESTAURANT_DISCOUNT_FLAT_OFF ,
GOLD_DISCOUNT ,
BRAND_PACK_DISCOUNT ,
TOTAL_AMOUNT ,
DISTANCE_KM ,
 created_at ,
    created_user ,
    updated_at ,
    updated_user 
)
values (
b.SALES_KEY ,
b.RESTRAUNT_KEY ,
b.ORDER_KEY ,
b.EMPLOYEE_KEY_CHEF ,
b.EMPLOYEE_KEY_WAITER ,
b.ITEM_KEY ,
b.BILL_SUBTOTAL ,
b.PACKAGING_CHARGES ,
b.RESTAURANT_DISCOUNT_PROMO ,
b.RESTAURANT_DISCOUNT_FLAT_OFF ,
b.GOLD_DISCOUNT ,
b.BRAND_PACK_DISCOUNT ,
b.TOTAL_AMOUNT ,
b.DISTANCE_KM ,
 b.created_at ,
   b.created_user ,
    b.updated_at ,
    b.updated_user 

)






{% endset %}

{% do run_query(sql) %}

{{ log("FACT SALES LOAD COMPLETED completed successfully.", info=True) }}

{% endmacro %}
