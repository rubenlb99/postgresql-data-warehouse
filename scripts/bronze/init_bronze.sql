TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
FROM '/tmp/crm/cust_info.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
FROM '/tmp/crm/prd_info.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
FROM '/tmp/crm/sales_details.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12 (cid, bdate, gen)
FROM '/tmp/erp/CUST_AZ12.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101 (id, cat, subcat, maintenance)
FROM '/tmp/erp/LOC_A101.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2 (cid, cntry)
FROM '/tmp/erp/PX_CAT_G1V2.csv'
DELIMITER ','
CSV HEADER;
