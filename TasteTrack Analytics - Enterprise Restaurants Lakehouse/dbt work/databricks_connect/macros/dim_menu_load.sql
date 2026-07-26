{% macro dim_menu_load() %}

{% set sql %}



MERGE INTO gold_layer.dim_menu as a using(

select 
md5(string(ITEM_NAME)) as ITEM_KEY,
ITEM_NAME,
ITEM_TYPE,
PRICE,
 'Y' as is_active,
    current_timestamp() as effective_from,
    '2050-07-26T12:10:41.480+00:00' as effective_to,
 current_timestamp() as created_at,
 'DBT' as created_user,
 md5(concat(ITEM_NAME,ITEM_TYPE,string(price))) as hashdiff
 from silver_layer.MENU

)
 as b on a.ITEM_KEY=b.ITEM_KEY and a.hashdiff<>b.hashdiff


when matched then update 
set
is_active='N',
effective_to=current_timestamp;


{% endset %}

{% do run_query(sql) %}

{% set sql %}

insert into gold_layer.dim_menu
select 
md5(ITEM_NAME) as item_key,
ITEM_NAME,
ITEM_TYPE,
PRICE,
 'Y' as is_active,
    current_timestamp() as effective_from,
    '2050-07-26T12:10:41.480+00:00' as effective_to,
 current_timestamp() as created_at,
 'DBT' as created_user,
md5(concat(ITEM_NAME,ITEM_TYPE,string(price))) as hashdiff
    
 from silver_layer.MENU;



{% endset %}

{% do run_query(sql) %}

{{ log("DIM MENU LOAD COMPLETED  completed successfully.", info=True) }}

{% endmacro %}