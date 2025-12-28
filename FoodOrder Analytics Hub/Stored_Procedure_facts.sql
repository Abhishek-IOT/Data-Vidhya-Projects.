drop procedure facts_load;
call facts_load();

DELIMITER $$

CREATE PROCEDURE facts_load()
BEGIN

    INSERT INTO BUSINESS.fact_order_patterns (
        customer_key,
        restaurant_key,
        date_key,
        order_count,
        total_spent,
        avg_basket_size,
        preferred_cuisine,
        meal_period,
        preferred_order_hour,
        preferred_order_day,
        preferred_delivery_address,
        avg_delivery_distance
    )
    SELECT
        dc.customer_key,
        dr.restaurant_key,
        DATE_FORMAT(o.order_date, '%Y%m%d') AS date_key,
        COUNT(*) AS order_count,
        SUM(o.total_amount) AS total_spent,
        AVG(o.total_amount) AS avg_basket_size,
        dr.cuisine_type AS preferred_cuisine,
        CASE 
            WHEN HOUR(o.order_date) BETWEEN 6 AND 11 THEN 'Breakfast'
            WHEN HOUR(o.order_date) BETWEEN 12 AND 16 THEN 'Lunch'
            WHEN HOUR(o.order_date) BETWEEN 17 AND 21 THEN 'Dinner'
            ELSE 'Late Night'
        END AS meal_period,
        HOUR(o.order_date) AS preferred_order_hour,
        DAYNAME(o.order_date) AS preferred_order_day,
        MAX(o.delivery_address) AS preferred_delivery_address,
        NULL AS avg_delivery_distance
    FROM Stage.stg_orders o
    JOIN BUSINESS.dim_customers dc 
        ON o.customer_id = dc.customer_id
    JOIN BUSINESS.dim_restaurants dr 
        ON o.restaurant_id = dr.restaurant_id
    GROUP BY
        dc.customer_key,
        dr.restaurant_key,
        date_key,
        preferred_order_hour,
        preferred_order_day,
        preferred_cuisine,
        meal_period;

    INSERT INTO Stage.etl_load_audit (
        job_name,
        source_schema,
        source_table,
        target_schema,
        target_table,
        load_start_time,
        load_status,
        executed_by,
        execution_date
    )
    VALUES (
        'JOB_stg_orders_TO_fact_order_patterns_LOAD',
        'Stage',
        'stg_orders',
        'BUSINESS',
        'fact_order_patterns',
        NOW(),
        'SUCCESS',
        'ETL_LOAD',
        CURDATE()
    );
    
    

END$$

DELIMITER ;


