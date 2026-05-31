use database STAGE_DEV;
create schema data_mart;
use schema data_mart;

CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_KEY           NUMBER AUTOINCREMENT,
    CUSTOMER_ID            STRING,
    CUSTOMER_NAME          STRING,
    EMAIL                  STRING,
    CITY                   STRING,
    STATE                  STRING,
    COUNTRY                STRING,
    CUSTOMER_STATUS        STRING,
    LOAD_DATE              TIMESTAMP,
    CREATED_TS            TIMESTAMP,
    UPDATED_TS            TIMESTAMP
);


CREATE OR REPLACE TABLE DIM_PRODUCT (
    PRODUCT_KEY            NUMBER AUTOINCREMENT,
    PRODUCT_ID             STRING,
    PRODUCT_NAME           STRING,
    CATEGORY               STRING,
    BRAND                  STRING,
    LOAD_DATE              TIMESTAMP,
    CREATED_TS            TIMESTAMP,
    UPDATED_TS            TIMESTAMP
);


CREATE OR REPLACE TABLE DIM_SELLER (
    SELLER_KEY             NUMBER AUTOINCREMENT,
    SELLER_ID              STRING,
    SELLER_NAME            STRING,
    SELLER_RATING          NUMBER(3,2),
    CITY                   STRING,
    COUNTRY                STRING,
    LOAD_DATE              TIMESTAMP,
    CREATED_TS            TIMESTAMP,
    UPDATED_TS            TIMESTAMP
);


CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY               NUMBER,
    FULL_DATE              DATE,
    DAY_NAME               STRING,
    MONTH_NAME             STRING,
    YEAR                   NUMBER,
    LOAD_DATE              TIMESTAMP

);


CREATE OR REPLACE TABLE FACT_SALES (
    SALES_KEY              NUMBER AUTOINCREMENT,

    CUSTOMER_KEY           NUMBER,
    PRODUCT_KEY            NUMBER,
    SELLER_KEY             NUMBER,
    DATE_KEY               NUMBER,

    ORDER_ID               STRING,
    QUANTITY               NUMBER,

    UNIT_PRICE             NUMBER(10,2),
    TOTAL_PRICE            NUMBER(12,2),
    LOAD_DATE              TIMESTAMP

);


CREATE OR REPLACE TABLE FACT_PRICE_HISTORY (
    PRICE_HISTORY_KEY      NUMBER AUTOINCREMENT,

    PRODUCT_KEY            NUMBER,
    SELLER_KEY             NUMBER,
    DATE_KEY               NUMBER,

    PRICE                  NUMBER(10,2),
    DISCOUNT_PERCENT       NUMBER(5,2),
    FINAL_PRICE            NUMBER(10,2),
    LOAD_DATE              TIMESTAMP
);