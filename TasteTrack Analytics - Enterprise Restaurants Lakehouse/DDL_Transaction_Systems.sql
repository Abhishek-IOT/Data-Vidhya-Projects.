create Schema Transactional_Data;


CREATE TABLE Transactional_Data.STG_RESTAURANT_ORDERS
(
    RESTAURANT_ID                            INT                 NOT NULL,
    RESTAURANT_NAME                          NVARCHAR(255)       NOT NULL,
    SUBZONE                                 NVARCHAR(255)       NULL,
    CITY                                    NVARCHAR(100)       NOT NULL,

    ORDER_ID                                BIGINT              NOT NULL,
    ORDER_PLACED_AT                         DATETIME2(0)        NOT NULL,
    ORDER_STATUS                            NVARCHAR(50)        NOT NULL,

    DELIVERY_PARTNER                        NVARCHAR(100)       NULL,
    DISTANCE_KM                             DECIMAL(5,2)        NULL,

    ITEM_NAME                               NVARCHAR(500)       NOT NULL,

    INSTRUCTIONS                            NVARCHAR(1000)      NULL,
    DISCOUNT_CONSTRUCT                      NVARCHAR(500)       NULL,

    BILL_SUBTOTAL                           DECIMAL(10,2)       NULL,
    PACKAGING_CHARGES                       DECIMAL(10,2)       NULL,

    RESTAURANT_DISCOUNT_PROMO               DECIMAL(10,2)       NULL,
    RESTAURANT_DISCOUNT_FLAT_OFF            DECIMAL(10,2)       NULL,
    GOLD_DISCOUNT                           DECIMAL(10,2)       NULL,
    BRAND_PACK_DISCOUNT                     DECIMAL(10,2)       NULL,

    TOTAL_AMOUNT                            DECIMAL(10,2)       NULL,

    RATING                                  DECIMAL(2,1)        NULL,
    REVIEW                                  NVARCHAR(MAX)       NULL,

    CANCELLATION_REJECTION_REASON           NVARCHAR(1000)      NULL,

    RESTAURANT_COMPENSATION_CANCELLATION    DECIMAL(10,2)       NULL,
    RESTAURANT_PENALTY_REJECTION            DECIMAL(10,2)       NULL,

    KPT_DURATION_MINUTES                    DECIMAL(6,2)        NULL,
    RIDER_WAIT_TIME_MINUTES                 DECIMAL(6,2)        NULL,

    ORDER_READY_MARKED                      NVARCHAR(50)        NULL,

    CUSTOMER_COMPLAINT_TAG                  NVARCHAR(500)       NULL,

    CUSTOMER_ID                             NVARCHAR(128)       NOT NULL,
     QUANTITY					 				DECIMAL(10,2)       NULL,
   
);