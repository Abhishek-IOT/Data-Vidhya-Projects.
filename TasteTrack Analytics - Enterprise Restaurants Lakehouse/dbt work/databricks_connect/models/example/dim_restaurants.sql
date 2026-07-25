

with dim_restuarants as 
(

    select 
    md5(concat(Restaurant_Name,Subzone,City)) as restaurant_key,
    Restaurant_Name,Subzone,city from silver_layer.restaurant_orders 
)
select * from dim_restuarants 
;
