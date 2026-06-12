create schema dm_view;


create or replace view dm_view.price_tracking_mobile as 
select date_key,dm.product_name,final_price from data_mart.fact_price_history fc
left join data_mart.dim_product dm on fc.product_key=dm.product_key 
where product_name in ('iPhone 15','Galaxy S24')
group by date_key,product_name,final_price order by product_name desc,final_price desc
;