INSERT INTO STG_CUSTOMERS
SELECT
    'CUST_' || LPAD(SEQ4()+1, 4, '0') AS CUSTOMER_ID,

    INITCAP(FN.FIRST_NAME || ' ' || LN.LAST_NAME) AS CUSTOMER_NAME,

    LOWER(FN.FIRST_NAME || '.' || LN.LAST_NAME ||
        CASE MOD(SEQ4(),4)
            WHEN 0 THEN '@gmail.com'
            WHEN 1 THEN '@yahoo.com'
            WHEN 2 THEN '@outlook.com'
            ELSE '@hotmail.com'
        END
    ) AS EMAIL,

    '9' || LPAD(UNIFORM(100000000,999999999,RANDOM()),9,'0') AS PHONE,

    CITY_DATA.CITY,
    CITY_DATA.STATE,
    'USA' AS COUNTRY,

    DATEADD(DAY, -UNIFORM(1,1200,RANDOM()), CURRENT_TIMESTAMP()),

    CASE WHEN MOD(SEQ4(),3)=0 THEN 'ACTIVE' ELSE 'INACTIVE' END,

    CURRENT_TIMESTAMP(),
    'SEED'
FROM TABLE(GENERATOR(ROWCOUNT => 100)) g,

LATERAL (
    SELECT
        ARRAY_CONSTRUCT(
            'James','John','Robert','Michael','William','David','Richard','Joseph',
            'Thomas','Charles','Mary','Patricia','Jennifer','Linda','Elizabeth',
            'Barbara','Susan','Jessica','Sarah','Karen'
        )[UNIFORM(0,19,RANDOM())] AS FIRST_NAME
) FN,

LATERAL (
    SELECT
        ARRAY_CONSTRUCT(
            'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller',
            'Davis','Rodriguez','Martinez','Hernandez','Lopez','Gonzalez',
            'Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin'
        )[UNIFORM(0,19,RANDOM())] AS LAST_NAME
) LN,

LATERAL (
    SELECT
        CASE MOD(SEQ4(),10)
            WHEN 0 THEN 'New York'
            WHEN 1 THEN 'Los Angeles'
            WHEN 2 THEN 'Chicago'
            WHEN 3 THEN 'Houston'
            WHEN 4 THEN 'Phoenix'
            WHEN 5 THEN 'Philadelphia'
            WHEN 6 THEN 'San Antonio'
            WHEN 7 THEN 'San Diego'
            WHEN 8 THEN 'Dallas'
            ELSE 'San Jose'
        END AS CITY,

        CASE MOD(SEQ4(),10)
            WHEN 0 THEN 'New York'
            WHEN 1 THEN 'California'
            WHEN 2 THEN 'Illinois'
            WHEN 3 THEN 'Texas'
            WHEN 4 THEN 'Arizona'
            WHEN 5 THEN 'Pennsylvania'
            WHEN 6 THEN 'Texas'
            WHEN 7 THEN 'California'
            WHEN 8 THEN 'Texas'
            ELSE 'California'
        END AS STATE
) CITY_DATA;






INSERT INTO STG_PRODUCTS
SELECT
    'PROD_' || LPAD(SEQ4()+1, 4, '0'),
    CASE MOD(SEQ4(),5)
        WHEN 0 THEN 'Laptop'
        WHEN 1 THEN 'Smartphone'
        WHEN 2 THEN 'Headphones'
        WHEN 3 THEN 'Shoes'
        ELSE 'Watch'
    END ,

    CASE MOD(SEQ4(),5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Electronics'
        WHEN 3 THEN 'Fashion'
        ELSE 'Accessories'
    END,

    CASE MOD(SEQ4(),4)
        WHEN 0 THEN 'Apple'
        WHEN 1 THEN 'Samsung'
        WHEN 2 THEN 'Nike'
        ELSE 'Adidas'
    END,

    ROUND(UNIFORM(50,2000,RANDOM()),2),
    CASE WHEN MOD(SEQ4(),4)=0 THEN 'ACTIVE' ELSE 'INACTIVE' END,
    CURRENT_TIMESTAMP(),
    'SEED'
FROM TABLE(GENERATOR(ROWCOUNT => 100));




INSERT INTO STG_SELLERS
SELECT
    'SELL_' || LPAD(SEQ4()+1, 4, '0') AS SELLER_ID,

    CASE MOD(SEQ4(),20)
        WHEN 0 THEN 'TechNova Electronics'
        WHEN 1 THEN 'UrbanCart USA'
        WHEN 2 THEN 'HomeNest Supplies'
        WHEN 3 THEN 'StyleForge Apparel'
        WHEN 4 THEN 'PrimeWave Traders'
        WHEN 5 THEN 'BrightMart Goods'
        WHEN 6 THEN 'NextGen Devices'
        WHEN 7 THEN 'AllDay Essentials'
        WHEN 8 THEN 'ElectroHub USA'
        WHEN 9 THEN 'FashionPeak Store'
        WHEN 10 THEN 'MegaSupply Co'
        WHEN 11 THEN 'UrbanEdge Retail'
        WHEN 12 THEN 'SmartZone Electronics'
        WHEN 13 THEN 'DailyNeeds Mart'
        WHEN 14 THEN 'ProGear Outfitters'
        WHEN 15 THEN 'HomeComfort Depot'
        WHEN 16 THEN 'Trendify Market'
        WHEN 17 THEN 'AlphaCommerce'
        WHEN 18 THEN 'Nimbus Retailers'
        ELSE 'Vertex Goods USA'
    END AS SELLER_NAME,

    ROUND(UNIFORM(3,5,RANDOM()),2) AS SELLER_RATING,

    CASE MOD(SEQ4(),10)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Houston'
        WHEN 4 THEN 'Phoenix'
        WHEN 5 THEN 'Philadelphia'
        WHEN 6 THEN 'San Antonio'
        WHEN 7 THEN 'San Diego'
        WHEN 8 THEN 'Dallas'
        ELSE 'San Jose'
    END AS CITY,

    'USA' AS COUNTRY,

    CURRENT_TIMESTAMP(),
    'SEED'
FROM TABLE(GENERATOR(ROWCOUNT => 100));



INSERT INTO STG_ORDERS
SELECT
    'ORD_' || LPAD(SEQ4()+1, 5, '0'),

    'CUST_' || LPAD(UNIFORM(1,100,RANDOM()),4,'0'),

    DATEADD(DAY, -UNIFORM(1,365,RANDOM()), CURRENT_TIMESTAMP()),

    CASE MOD(SEQ4(),4)
        WHEN 0 THEN 'PLACED'
        WHEN 1 THEN 'SHIPPED'
        WHEN 2 THEN 'DELIVERED'
        ELSE 'CANCELLED'
    END,

    0, -- placeholder, will be updated later

    CURRENT_TIMESTAMP(),
    'SEED'
FROM TABLE(GENERATOR(ROWCOUNT => 100));


INSERT INTO STG_ORDER_ITEMS
SELECT
    o.ORDER_ID,
    p.PRODUCT_ID,
    'SELL_' || LPAD(UNIFORM(1,100,RANDOM()),4,'0'),

    UNIFORM(1,5,RANDOM()) AS QUANTITY,
    p.BASE_PRICE AS UNIT_PRICE,
    (UNIFORM(1,5,RANDOM()) * p.BASE_PRICE) AS TOTAL_PRICE,

    CURRENT_TIMESTAMP(),
    'SEED'
FROM STG_ORDERS o
JOIN TABLE(GENERATOR(ROWCOUNT => 3)) g
JOIN STG_PRODUCTS p
WHERE UNIFORM(1,10,RANDOM()) <= 6;




UPDATE STG_ORDERS o
SET TOTAL_AMOUNT = (
    SELECT SUM(TOTAL_PRICE)
    FROM STG_ORDER_ITEMS i
    WHERE i.ORDER_ID = o.ORDER_ID
);



INSERT INTO STG_PRICE_UPDATES
SELECT
    'PROD_' || LPAD(UNIFORM(1,100,RANDOM()),4,'0'),
    ROUND(UNIFORM(50,2000,RANDOM()),2),
    ROUND(UNIFORM(0,30,RANDOM()),2),

    ROUND(
        UNIFORM(50,2000,RANDOM()) -
        (UNIFORM(0,30,RANDOM())/100 * UNIFORM(50,2000,RANDOM()))
    ,2),

    DATEADD(HOUR, -UNIFORM(1,1000,RANDOM()), CURRENT_TIMESTAMP()),

    CURRENT_TIMESTAMP(),
    'SEED'
FROM TABLE(GENERATOR(ROWCOUNT => 100));



INSERT INTO STG_PROMOTIONS
SELECT
    'PROMO_' || LPAD(SEQ4()+1,4,'0') AS PROMOTION_ID,
    'PROD_' || LPAD(UNIFORM(1,100,RANDOM()),4,'0') as PRODUCT_ID,
    CASE MOD(SEQ4(),20)
        WHEN 0 THEN 'Back to School Mega Deals'
        WHEN 1 THEN 'Black Friday Early Access'
        WHEN 2 THEN 'Cyber Monday Specials'
        WHEN 3 THEN 'Holiday Clearance Event'
        WHEN 4 THEN 'New Year Tech Bonanza'
        WHEN 5 THEN 'Weekend Flash Sale'
        WHEN 6 THEN 'Summer Kickoff Deals'
        WHEN 7 THEN 'Winter Essentials Discount'
        WHEN 8 THEN 'Spring Refresh Sale'
        WHEN 9 THEN 'Labor Day Savings'
        WHEN 10 THEN 'Independence Day Offers'
        WHEN 11 THEN 'Memorial Day Deals'
        WHEN 12 THEN 'Valentine Special Offers'
        WHEN 13 THEN 'Prime Member Exclusive'
        WHEN 14 THEN 'Electronics Mega Discount'
        WHEN 15 THEN 'Fashion Fiesta Sale'
        WHEN 16 THEN 'Home Makeover Campaign'
        WHEN 17 THEN 'Daily Lightning Deals'
        WHEN 18 THEN 'Clearance Blowout Event'
        ELSE 'Limited Time Super Saver'
    END AS PROMOTION_NAME,

    ROUND(UNIFORM(5,40,RANDOM()),2) AS DISCOUNT_PERCENT,

    DATEADD(DAY, -UNIFORM(10,200,RANDOM()), CURRENT_TIMESTAMP()),
    DATEADD(DAY, UNIFORM(10,200,RANDOM()), CURRENT_TIMESTAMP()),

    CURRENT_TIMESTAMP(),
    'SEED'
FROM TABLE(GENERATOR(ROWCOUNT => 100));