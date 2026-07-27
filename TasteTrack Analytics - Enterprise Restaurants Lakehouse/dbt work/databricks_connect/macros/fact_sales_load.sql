{% macro fact_sales_load() %}

{% set sql %}






{% endset %}

{% do run_query(sql) %}

{{ log("DIM EMPLOYEE LOAD COMPLETED completed successfully.", info=True) }}

{% endmacro %}
