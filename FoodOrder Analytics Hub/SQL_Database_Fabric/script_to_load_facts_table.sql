-- Declare variables
DECLARE @row_count_before BIGINT;
DECLARE @row_count_after  BIGINT;
DECLARE @load_start_ts    DATETIME2(6);
DECLARE @load_end_ts      DATETIME2(6);
DECLARE @audit_id         BIGINT;


-- Capture load start time
SELECT @load_start_ts = CURRENT_TIMESTAMP;

-- Count before load
SELECT @row_count_before = COUNT(*)
FROM business.fact_order_patterns;


  INSERT INTO business.fact_order_patterns (
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
        FORMAT(o.order_date, 'yyyymmdd') AS date_key,
        COUNT(*) AS order_count,
        SUM(o.total_amount) AS total_spent,
        AVG(o.total_amount) AS avg_basket_size,
        dr.cuisine_type AS preferred_cuisine,
        CASE 
            WHEN datepart(HOUR,o.order_date) BETWEEN 6 AND 11 THEN 'Breakfast'
            WHEN datepart(HOUR,o.order_date) BETWEEN 12 AND 16 THEN 'Lunch'
            WHEN datepart(HOUR,o.order_date) BETWEEN 17 AND 21 THEN 'Dinner'
            ELSE 'Late Night'
        END AS meal_period,	
        datepart(HOUR,o.order_date) AS preferred_order_hour,
        datename(weekday,o.order_date) AS preferred_order_day,
        MAX(o.delivery_address) AS preferred_delivery_address,
        NULL AS avg_delivery_distance
    FROM Stage.stg_orders o
    JOIN business.dim_customers dc 
        ON o.customer_id = dc.customer_id
    JOIN business.dim_restaurants dr 
        ON o.restaurant_id = dr.restaurant_id
  GROUP BY
    dc.customer_key,
    dr.restaurant_key,
    FORMAT(o.order_date, 'yyyymmdd'),
    dr.cuisine_type,
	o.order_date,
    DATEPART(HOUR, o.order_date),
    DATENAME(WEEKDAY, o.order_date),
    CASE
        WHEN DATEPART(HOUR, o.order_date) BETWEEN 6 AND 11 THEN 'Breakfast'
        WHEN DATEPART(HOUR, o.order_date) BETWEEN 12 AND 16 THEN 'Lunch'
        WHEN DATEPART(HOUR, o.order_date) BETWEEN 17 AND 21 THEN 'Dinner'
        ELSE 'Late Night'
    END;

		
		
SELECT @row_count_after = COUNT(*)
FROM business.fact_order_patterns;

-- Capture load end time
SELECT @load_end_ts = CURRENT_TIMESTAMP;		
