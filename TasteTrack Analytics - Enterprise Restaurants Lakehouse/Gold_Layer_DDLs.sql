

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
    Restaurant_Name string,
    Subzone string,
    City string,
    created_at timestamp,
    created_user string,
    updated_at timestamp,
    updated_user string

);



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
effective_to timestamp

);