use database dev;
create schema VAULT;
use schema VAULT;



---Hub Tables---
CREATE OR REPLACE TABLE HUB_CUSTOMER (
    CUSTOMER_HK            STRING,
    CUSTOMER_ID            STRING,
    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE HUB_PRODUCT (
    PRODUCT_HK             STRING,
    PRODUCT_ID             STRING,
    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE HUB_ORDER (
    ORDER_HK               STRING,
    ORDER_ID               STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE HUB_SELLER (
    SELLER_HK              STRING,
    SELLER_ID              STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE HUB_PROMOTION (
    PROMOTION_HK           STRING,
    PROMOTION_ID           STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);




---Link Tables---
CREATE OR REPLACE TABLE LINK_CUSTOMER_ORDER (
    CUSTOMER_ORDER_HK      STRING,

    CUSTOMER_HK            STRING,
    ORDER_HK               STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE LINK_PRODUCT_SELLER (
    PRODUCT_SELLER_HK      STRING,

    PRODUCT_HK             STRING,
    SELLER_HK              STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE LINK_PRODUCT_PROMOTION (
    PRODUCT_PROMOTION_HK   STRING,

    PRODUCT_HK             STRING,
    PROMOTION_HK           STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE LINK_ORDER_PRODUCT (
    ORDER_PRODUCT_HK      STRING,

    PRODUCT_HK             STRING,
    ORDER_HK              STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE LINK_ORDER_PROMOTION (
    ORDER_PROMOTION_HK      STRING,

    ORDER_HK             STRING,
    PROMOTION_HK              STRING,

    LOAD_DATE              TIMESTAMP,
    RECORD_SOURCE          STRING
);




---SAT Tables---
CREATE OR REPLACE TABLE SAT_CUSTOMER_DETAILS (
    CUSTOMER_HK            STRING,

    CUSTOMER_NAME          STRING,
    EMAIL                  STRING,
    PHONE                  STRING,
    CITY                   STRING,
    STATE                  STRING,
    COUNTRY                STRING,
    CUSTOMER_STATUS        STRING,

    HASHDIFF               STRING,
    LOAD_DATE              TIMESTAMP,
    EFFECTIVE_FROM         TIMESTAMP,
    RECORD_SOURCE          STRING
);

CREATE OR REPLACE TABLE SAT_PRODUCT_DETAILS (
    PRODUCT_HK             STRING,

    PRODUCT_NAME           STRING,
    CATEGORY               STRING,
    BRAND                  STRING,
    PRODUCT_STATUS         STRING,

    HASHDIFF               STRING,
    LOAD_DATE              TIMESTAMP,
    EFFECTIVE_FROM         TIMESTAMP,
    RECORD_SOURCE          STRING
);



CREATE OR REPLACE TABLE SAT_ORDER_DETAILS (
    ORDER_HK               STRING,

    ORDER_STATUS           STRING,
    TOTAL_AMOUNT           NUMBER(12,2),

    HASHDIFF               STRING,
    LOAD_DATE              TIMESTAMP,
    EFFECTIVE_FROM         TIMESTAMP,
    RECORD_SOURCE          STRING
);

CREATE OR REPLACE TABLE SAT_PRODUCT_PRICE_HISTORY (
    PRODUCT_HK             STRING,
    SELLER_HK              STRING,

    PRICE                  NUMBER(10,2),
    DISCOUNT_PERCENT       NUMBER(5,2),
    FINAL_PRICE            NUMBER(10,2),

    HASHDIFF               STRING,
    PRICE_UPDATE_TIME      TIMESTAMP,
    LOAD_DATE              TIMESTAMP,
    EFFECTIVE_FROM         TIMESTAMP,
    RECORD_SOURCE          STRING
);


CREATE OR REPLACE TABLE SAT_PROMOTION_DETAILS (
    PROMOTION_HK           STRING,

    PROMOTION_NAME         STRING,
    DISCOUNT_PERCENT       NUMBER(5,2),
    START_DATE             TIMESTAMP,
    END_DATE               TIMESTAMP,

    HASHDIFF               STRING,
    LOAD_DATE              TIMESTAMP,
    EFFECTIVE_FROM         TIMESTAMP,
    RECORD_SOURCE          STRING
);



CREATE OR REPLACE TABLE SAT_SELLER_DETAILS (
    SELLER_HK              STRING,

    SELLER_NAME            STRING,
    SELLER_RATING          NUMBER(3,2),
    CITY                   STRING,
    COUNTRY                STRING,

    HASHDIFF               STRING,
    LOAD_DATE              TIMESTAMP,
    EFFECTIVE_FROM         TIMESTAMP,
    RECORD_SOURCE          STRING
);
