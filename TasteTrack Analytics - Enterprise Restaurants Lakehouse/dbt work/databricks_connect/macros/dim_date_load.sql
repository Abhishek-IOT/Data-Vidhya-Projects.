{% macro dim_date_load() %}

{% set sql %}



INSERT INTO gold_layer.DIM_DATE
SELECT
    CAST(date_format(FULL_DATE, 'yyyyMMdd') AS BIGINT) AS DATE_KEY,
    FULL_DATE,
    date_format(FULL_DATE, 'dd') AS DAY_NUMBER,
    month(FULL_DATE) AS MONTH_NUMBER,
    date_format(FULL_DATE, 'MMMM') AS MONTH_NAME,
    CONCAT('Q', quarter(FULL_DATE)) AS QUARTER_NUMBER,
    year(FULL_DATE) AS YEAR_NUMBER,
    current_timestamp() AS LOAD_DATE
FROM (
    SELECT EXPLODE(
        SEQUENCE(
            TO_DATE('2020-01-01'),
            CURRENT_DATE(),
            INTERVAL 1 DAY
        )
    ) AS FULL_DATE
);








{% endset %}

{% do run_query(sql) %}

{{ log("Dim Date LOAD COMPLETED completed successfully.", info=True) }}

{% endmacro %}
