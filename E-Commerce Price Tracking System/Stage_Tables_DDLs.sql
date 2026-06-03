create database DEVELOPMENT;
create schema STAGE_DEV;


CREATE OR REPLACE TABLE STG_CUSTOMERS (
    CUSTOMER_ID            STRING,
    CUSTOMER_NAME          STRING,
    EMAIL                  STRING,
    PHONE                  STRING,
    CITY                   STRING,
    STATE                  STRING,
    COUNTRY                STRING,
    REGISTERED_DATE        TIMESTAMP,
    CUSTOMER_STATUS        STRING,
    LOAD_DATE              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE STG_PRODUCTS (
    PRODUCT_ID             STRING,
    PRODUCT_NAME           STRING,
    CATEGORY               STRING,
    BRAND                  STRING,
    BASE_PRICE             NUMBER(10,2),
    PRODUCT_STATUS         STRING,
    LOAD_DATE              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE          STRING
);



CREATE OR REPLACE TABLE STG_PRICE_UPDATES (
    PRODUCT_ID             STRING,
    PRICE                  NUMBER(10,2),
    DISCOUNT_PERCENT       NUMBER(5,2),
    FINAL_PRICE            NUMBER(10,2),
    PRICE_UPDATE_TIME      TIMESTAMP,
    LOAD_DATE              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE          STRING
);



CREATE OR REPLACE TABLE STG_ORDERS (
    ORDER_ID               STRING,
    CUSTOMER_ID            STRING,
    ORDER_DATE             TIMESTAMP,
    ORDER_STATUS           STRING,
    TOTAL_AMOUNT           NUMBER(12,2),
    LOAD_DATE              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE STAGE_DEV.STG_PRICE_UPDATES (
    PRODUCT_ID             STRING,
    PRICE                  NUMBER(10,2),
    DISCOUNT_PERCENT       NUMBER(5,2),
    FINAL_PRICE            NUMBER(10,2),
    PRICE_UPDATE_TIME      TIMESTAMP_ntz(9),
    LOAD_DATE              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE STG_SELLERS (
    SELLER_ID              STRING,
    SELLER_NAME            STRING,
    SELLER_RATING          NUMBER(3,2),
    CITY                   STRING,
    COUNTRY                STRING,

    LOAD_DATE              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE          STRING
);



CREATE OR REPLACE TABLE STAGE_DEV.STG_PROMOTIONS (
    PROMOTION_ID           STRING,
    PRODUCT_ID             STRING,
    PROMOTION_NAME         STRING,
    DISCOUNT_PERCENT       NUMBER(5,2),
    START_DATE             TIMESTAMP,
    END_DATE               TIMESTAMP,

    LOAD_DATE              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    RECORD_SOURCE          STRING
);


