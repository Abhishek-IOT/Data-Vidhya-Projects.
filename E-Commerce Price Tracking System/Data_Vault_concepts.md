# HUB Tables

# Purpose

- Store unique business keys only.
- Examples:
    Customer ID
    Product ID
    Order ID

HUB_CUSTOMER
------------
CUSTOMER_HK
CUSTOMER_ID
LOAD_DATE
RECORD_SOURCE

Why we use hub tables?

If customer details change (name, city, phone), the Hub remains unchanged because the business key is still C001.

# SATELLITE Tables
# Purpose
- Store descriptive attributes and history.
- Examples:
    Customer Name
    Email
    City
    Status

Structure
SAT_CUSTOMER_DETAILS
--------------------
CUSTOMER_HK
CUSTOMER_NAME
EMAIL
CITY
HASHDIFF
LOAD_DATE

Satellite tables can be have the history of the records. For Example : if customer changes it's city then we can insert a new record into Satellite table and make it the latest record.

# LINK Tables
# Purpose
- Store relationships between business entities.
- Examples:
    Customer buys Product
    Order contains Product
    Seller sells Product

Structure
LINK_ORDER_PRODUCT
------------------
LINK_HK
ORDER_HK
PRODUCT_HK
LOAD_DATE

It has the business keys of order hub and the product hub.