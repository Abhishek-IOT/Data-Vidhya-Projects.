{% macro fact_sales_load() %}

{% set sql %}



-- MERGE INTO gold_layer.fact_sales a
-- USING (
SELECT 
MD5(CONCAT(dr.restaurant_key,ddo.order_key)) AS SALES_KEY,
--ORDER_KEY,EMPLOYEE_KEY_CHEF,EMPLOYEE_KEY_WAITER,EMPLOYEE_KEY_MANAGER
dr.restaurant_key as RESTRAUNT_KEY,
ddo.order_key,
de1.EMPLOYEE_KEY as EMPLOYEE_KEY_CHEF,
de2.EMPLOYEE_KEY as EMPLOYEE_KEY_WAITER,
dm.ITEM_KEY,
base.BILL_SUBTOTAL,
base.PACKAGING_CHARGES,
base.RESTAURANT_DISCOUNT_PROMO,
base.RESTAURANT_DISCOUNT_FLAT_OFF,
base.GOLD_DISCOUNT,
base.BRAND_PACK_DISCOUNT,
base.TOTAL_AMOUNT,
base.DISTANCE_KM,
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
base.item_name=dm.ITEM_NAME
;







{% endset %}

{% do run_query(sql) %}

{{ log("FACT SALES LOAD COMPLETED completed successfully.", info=True) }}

{% endmacro %}
