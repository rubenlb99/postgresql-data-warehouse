-- customer dim

select 
	ci.cst_id as CUSTOMER_ID,
	ci.cst_key AS CUSTOMER_KEY,
	ci.cst_firstname AS FIRSTNAME,
	ci.cst_lastname AS LASTNAME,
	ci.cst_marital_status AS MARITAL_STATUS,
	ci.cst_gndr AS GENDER, 
	caz.bdate AS BIRTHDAY,
	cloc.cntry AS COUNTRY,
	ci.cst_create_date AS CREATED_DATE
from silver.crm_cust_info ci
join silver.erp_cust_az12 caz
on ci.cst_key = caz.cid
join silver.erp_loc_a101 cloc
on ci.cst_key = cloc.cid
