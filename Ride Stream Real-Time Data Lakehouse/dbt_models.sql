#sources for now it is aqi_db but we need to change this

version: 2

sources:
  - name: taxi_source
    database: awsdatacatalog   # the catalog (always this in Athena)
    schema: aqi_db             # your Glue database
    tables:
      - name: facts            # your Glue table name


#Total revenue by vendor
Firstly create the file and put the query :
vi total_revenue.sql

{{ config(materialized='table') }}

select
    v.vendor_id,
    sum(f.total_revenue) as revenue
from {{ source('raw', 'facts') }} f
join {{ source('raw', 'dim_vendor') }} v
    on f.vendor_key = v.vendor_key
group by v.vendor_id
order by revenue desc


dbt run --select total_revenue