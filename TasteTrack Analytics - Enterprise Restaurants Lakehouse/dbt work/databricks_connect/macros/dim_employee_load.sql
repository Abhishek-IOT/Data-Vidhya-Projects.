{% macro dim_employee_load() %}

{% set sql %}

MERGE INTO GOLD_LAYER.dim_employee as a using 
(
 select md5(EMPLOYEE_NAME) as EMPLOYEE_KEY,
 EMPLOYEE_ID,
    EMPLOYEE_NAME,
    Monthly_Salary AS SALARY,
    Salary_Band AS BAND,
    Manager_Name AS MANAGER,
    RATING,
    DESIGNATION,
    'Y' as is_active,
    current_timestamp() as effective_from,
    '2050-07-26T12:10:41.480+00:00' as effective_to,
    md5(concat(EMPLOYEE_NAME,string(SALARY),band,manager,rating,DESIGNATION)) as hashdiff,
    current_timestamp as created_at,
    'DBT' AS created_user
    from silver_layer.employee
)
as b on a.EMPLOYEE_KEY=b.EMPLOYEE_KEY and a.hashdiff<>b.hashdiff

when matched then update 
set 
a.EMPLOYEE_ID=b.EMPLOYEE_ID
a.EMPLOYEE_NAME=b.EMPLOYEE_NAME,
a.SALARY=b.SALARY,
a.BAND=b.band,
a.MANAGER=b.manager,
a.RATING=b.RATING,
a.DESIGNATION=b.DESIGNATION,
is_active='N',
 effective_to=current_timestamp()

when not matched then INSERT
(
EMPLOYEE_KEY,
EMPLOYEE_ID
EMPLOYEE_NAME,
SALARY,
BAND,
MANAGER,
RATING,
DESIGNATION,
is_active,
effective_from,
effective_to,
hashdiff,
created_at,
created_user
)
VALUES
(
b.EMPLOYEE_KEY,
b.EMPLOYEE_ID,
b.EMPLOYEE_NAME,
b.SALARY,
b.BAND,
b.MANAGER,
b.RATING,
b.DESIGNATION,
b.is_active,
b.effective_from,
b.effective_to,
b.hashdiff,
b.created_at,
b.created_user
)
{% endset %}

{% do run_query(sql) %}

{{ log("DIM EMPLOYEE LOAD COMPLETED completed successfully.", info=True) }}

{% endmacro %}
