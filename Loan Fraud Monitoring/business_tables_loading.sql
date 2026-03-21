

MERGE INTO business.dim_customer AS tgt
USING (
    SELECT DISTINCT
        customer_id,
        full_name,
        DATEDIFF(YEAR, date_of_birth, GETDATE()) AS age,
        gender,
        city,
        state,
        employment_type,
        annual_income,
        kyc_status,
        customer_status
    FROM Stage.stg_customer
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN
    UPDATE SET
        tgt.full_name = src.full_name,
        tgt.age = src.age,
        tgt.city = src.city,
        tgt.state = src.state,
        tgt.annual_income = src.annual_income,
        tgt.kyc_status = src.kyc_status

WHEN NOT MATCHED THEN
    INSERT (
        customer_id, full_name, age, gender, city, state,
        employment_type, annual_income, kyc_status, customer_status,
        effective_from, effective_to, is_current
    )
    VALUES (
        src.customer_id, src.full_name, src.age, src.gender, src.city, src.state,
        src.employment_type, src.annual_income, src.kyc_status, src.customer_status,
        GETDATE(), NULL, 1
    );




MERGE INTO Business.dim_credit_profile AS tgt
USING (
    SELECT
        customer_id,
        credit_score,
        CASE 
            WHEN credit_score >= 750 THEN 'Excellent'
            WHEN credit_score >= 650 THEN 'Good'
            ELSE 'Risky'
        END AS credit_score_band,
        past_defaults,
        active_loans,
        enquiries_last_6m,
        report_date
    FROM Stage.stg_credit_bureau
) AS src
ON tgt.customer_id = src.customer_id
   AND tgt.report_date = src.report_date

WHEN NOT MATCHED THEN
    INSERT (
        customer_id, credit_score, credit_score_band,
        past_defaults, active_loans, enquiries_last_6m, report_date
    )
    VALUES (
        src.customer_id, src.credit_score, src.credit_score_band,
        src.past_defaults, src.active_loans, src.enquiries_last_6m, src.report_date
    );




INSERT INTO Gold.fact_loan_application
(
    application_id,
    customer_key,
    device_key,
    credit_key,
    date_key,
    requested_amount,
    tenure_months,
    interest_rate,
    fraud_flag,
    fraud_reason
)
SELECT
    a.application_id,

    c.customer_key,
    d.device_key,
    cr.credit_key,

    CONVERT(INT, FORMAT(a.application_ts, 'yyyyMMdd')) AS date_key,

    a.requested_amount,
    a.tenure_months,
    a.interest_rate,

    -- 🔥 Simple Fraud Logic
    CASE 
        WHEN cr.credit_score < 600 THEN 1
        WHEN a.requested_amount > 1000000 THEN 1
        WHEN a.channel = 'WEB' AND d.device_type = 'UNKNOWN' THEN 1
        ELSE 0
    END AS fraud_flag,

    CASE 
        WHEN cr.credit_score < 600 THEN 'Low Credit Score'
        WHEN a.requested_amount > 1000000 THEN 'High Loan Amount'
        ELSE NULL
    END AS fraud_reason

FROM Stage.stg_loan_application a

LEFT JOIN Gold.dim_customer c
    ON a.customer_id = c.customer_id
    AND c.is_current = 1

LEFT JOIN Gold.dim_device d
    ON a.device_id = d.device_id

LEFT JOIN Gold.dim_credit_profile cr
    ON a.customer_id = cr.customer_id;