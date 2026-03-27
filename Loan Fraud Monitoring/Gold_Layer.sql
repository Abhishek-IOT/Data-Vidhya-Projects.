
--Number of Loans Applied from different Devices :

select DEVICE_TYPE,count(*)
from BUSINESS.FACT_LOAN_APPLICATION fc
join BUSINESS.DIM_DEVICE dm
on fc.DEVICE_KEY=dm.DEVICE_KEY
group by DEVICE_TYPE
;


--Number of ACtive Loans PEr Customer :

select ACTIVE_LOANS,fc.full_name
from  BUSINESS.DIM_CUSTOMER fc 
join BUSINESS.DIM_CREDIT_PROFILE dm
on fc.CUSTOMER_ID=dm.CUSTOMER_ID
;



--Fraudulent Loan Applications :

select count(*) from BUSINESS.FACT_LOAN_APPLICATION
where fraud_flag=1
 ;


-- Number of Risky Loan Applications :
select 
count(*)
 from
 Business.fact_loan_application fc 
 left join  Business.dim_credit_profile dm 
 on dm.credit_key=fc.credit_key
 where credit_score_band='Risky'
 ;


-- Age Group of Customers Applying for Loans :
select Age_Groups,count(*) from 
(
select 
case when age>=18 and age<=30 then 'Early Starters'
 when age>=31 and age<=60 then 'Mid Rangers'
else 'Senior Citizens' end as Age_Groups
from Business.fact_loan_application fc
inner join business.dim_customer dm
on fc.customer_key=dm.customer_key
) as T Group by Age_Groups
;
