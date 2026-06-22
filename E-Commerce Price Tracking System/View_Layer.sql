create schema dm_view;


create or replace view Product_Price_Comparision as 
select product_name,max(final_price) as Maximum_Price,min(final_price) as Minimum_Price,avg(final_price) as Average_Price from data_mart.FACT_PRICE_HISTORY fc
left join data_mart.dim_product dm1 on fc.product_key=dm1.product_key
left join data_mart.dim_seller dm2 on fc.seller_key=dm2.seller_key
group by 1;




create or replace view max_discount_offer_on_product as 
select product_name,max(discount_percent) as discount from data_mart.FACT_PRICE_HISTORY fc
left join data_mart.dim_product dm1 on fc.product_key=dm1.product_key
group by 1 order by 2 desc
;

create or replace view Product_Price_Over_Time as 
select  date_key,dm.product_name as product_name,min(final_price) as minimum_final_price_of_day from data_mart.fact_price_history fc
left join data_mart.dim_product dm on fc.product_key=dm.product_key 
group by date_key,product_name,final_price order by product_name desc,final_price desc;


 create or replace view quantity_sold_per_product as 
select product_name,sum(quantity) from data_mart.fact_sales fc
left join data_mart.dim_product dm1 on fc.product_key=dm1.product_key
group by 1
;


create or replace view dm_view.seller_rating as 
select distinct seller_name,seller_rating from data_mart.dim_seller;



update data_mart.FACT_PRICE_HISTORY
set date_key='20260620' where final_price='799.20';

update data_mart.FACT_PRICE_HISTORY
set date_key='20260622' where final_price='949.05';

update data_mart.FACT_PRICE_HISTORY
set date_key='20260622' where final_price='854.05';


update data_mart.FACT_PRICE_HISTORY
set date_key='20260622' where final_price='799.00';

update data_mart.FACT_PRICE_HISTORY
set date_key='20260620' where final_price='679.15';

update data_mart.FACT_PRICE_HISTORY
set date_key='20260620' where final_price='120.00';

update data_mart.FACT_PRICE_HISTORY
set date_key='20260622' where final_price='48';

update data_mart.FACT_PRICE_HISTORY
set date_key='20260622' where final_price='630';

update data_mart.FACT_PRICE_HISTORY
set discount_percent='40' where product_key in (428,429);

update data_mart.FACT_PRICE_HISTORY
set discount_percent='22' where product_key in (415);