{% macro fact_sales_load() %}

{% set sql %}






{% endset %}

{% do run_query(sql) %}

{{ log("FACT SALES LOAD COMPLETED completed successfully.", info=True) }}

{% endmacro %}
