create schema dm_view;


create or replace view dm_view.price_tracking_mobile as 
select date_key,dm.product_name,final_price from data_mart.fact_price_history fc
left join data_mart.dim_product dm on fc.product_key=dm.product_key 
group by date_key,product_name,final_price order by product_name desc,final_price desc
;


create or replace view dm_view.seller_price_comparison as 
SELECT
    dm1.PRODUCT_NAME,
    dm2.SELLER_NAME,
    min(unit_price) as  AVG_PRICE
FROM data_mart.fact_sales fc
left join data_mart.dim_product dm1 on fc.product_key=dm1.product_key
left join data_mart.dim_seller dm2 on fc.seller_key=dm2.seller_key
GROUP BY ALL;


create or replace view dm_view.maximum_discount_by_seller as 
select product_name,seller_name,max(discount_percent) from data_mart.FACT_PRICE_HISTORY fc
left join data_mart.dim_product dm1 on fc.product_key=dm1.product_key
left join data_mart.dim_seller dm2 on fc.seller_key=dm2.seller_key
group by 1,2
;


create or replace view dm_view.seller_rating as 
select distinct seller_name,seller_rating from data_mart.dim_seller;
