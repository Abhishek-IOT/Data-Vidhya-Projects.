{% macro load_orders() %}

{% set sql %}

MERGE INTO adf.gold_layer.dim_restaurants AS A
USING (
    SELECT
        md5(concat(Restaurant_Name, Subzone, City)) AS restaurant_key,
        Restaurant_Name,
        Subzone,
        current_timestamp() AS created_at,
        'DBT' AS created_user,
        current_timestamp() AS updated_at,
        'DBT' AS updated_user,
        City
    FROM silver_layer.restaurant_orders
) AS B
ON B.restaurant_key = A.restaurant_key

WHEN MATCHED THEN
UPDATE SET
    A.Restaurant_Name = B.Restaurant_Name,
    A.Subzone = B.Subzone,
    A.City = B.City,
    A.updated_at = current_timestamp()

WHEN NOT MATCHED THEN
INSERT (
    restaurant_key,
    Restaurant_Name,
    Subzone,
    City,
    created_at,
    created_user,
    updated_at,
    updated_user
)
VALUES (
    B.restaurant_key,
    B.Restaurant_Name,
    B.Subzone,
    B.City,
    B.created_at,
    B.created_user,
    B.updated_at,
    B.updated_user
);

{% endset %}

{% do run_query(sql) %}

{{ log("DIM RESTAURANT LOAD COMPLETED  completed successfully.", info=True) }}

{% endmacro %}