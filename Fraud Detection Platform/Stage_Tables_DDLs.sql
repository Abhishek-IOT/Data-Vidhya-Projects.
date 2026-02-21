create schema Stage;

SELECT * from Stage.stg_customer;

CREATE TABLE Stage.stg_customer
(
    customer_id         VARCHAR(50),
    full_name           VARCHAR(200),
    date_of_birth       DATE,
    gender              VARCHAR(20),
    city                VARCHAR(100),
    state               VARCHAR(100),
    pincode             VARCHAR(20),
    employment_type     VARCHAR(50),
    annual_income       DECIMAL(18,2),
    account_open_date   DATE,
    kyc_status          VARCHAR(20),      -- VERIFIED / PENDING
    customer_status     VARCHAR(20),      -- ACTIVE / CLOSED
    ingestion_ts        DATETIME2
);


CREATE TABLE Stage.stg_loan_application
(
    application_id      VARCHAR(100),
    customer_id         VARCHAR(100),
    application_ts      DATETIME2(6),
    channel             VARCHAR(50),      -- MOBILE / WEB / BRANCH
    loan_product        VARCHAR(50),      -- PERSONAL / HOME / AUTO
    requested_amount    DECIMAL(18,2),
    tenure_months       INT,
    interest_rate       DECIMAL(5,2),
    purpose             VARCHAR(200),
    branch_id           VARCHAR(50),
    application_status  VARCHAR(50),      -- SUBMITTED
    ingestion_ts        DATETIME2(6)
);



CREATE TABLE Stage.stg_application_device
(
    application_id      VARCHAR(100),
    device_id           VARCHAR(100),
    device_type         VARCHAR(100),      -- Android / iPhone / Laptop
    os_version          VARCHAR(100),
    browser             VARCHAR(100),
    ip_address          VARCHAR(100),
    latitude            DECIMAL(5,2),
    longitude           DECIMAL(5,2),
    event_ts            DATETIME2(6),
    ingestion_ts        DATETIME2(6)
);



CREATE TABLE Stage.stg_credit_bureau
(
    customer_id         varchar(100),
    credit_score        INT,
    total_accounts      INT,
    active_loans        INT,
    past_defaults       INT,
    enquiries_last_6m   INT,
    bureau_source       varchar(100),
    report_date         DATE
);
