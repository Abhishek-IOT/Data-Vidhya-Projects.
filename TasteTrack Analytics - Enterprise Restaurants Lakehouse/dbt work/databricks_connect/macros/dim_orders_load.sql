{% macro dim_orders_load() %}

{% set sql %}


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



{% endset %}

{% do run_query(sql) %}

{{ log("MERGE completed successfully.", info=True) }}

{% endmacro %}