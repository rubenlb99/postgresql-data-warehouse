DROP TABLE IF EXISTS bronze.crm_cust_info;
create table bronze.crm_cust_info (
	cst_id              INTEGER,
    cst_key             VARCHAR(20),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  CHAR(1),         -- E.g. M, S
    cst_gndr            CHAR(1),         -- E.g. M, F
    cst_create_date     DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
create table bronze.crm_prd_info (
	prd_id         INTEGER,
    prd_key        VARCHAR(30),
    prd_nm         VARCHAR(100),
    prd_cost       NUMERIC(10,2),    
    prd_line       VARCHAR(10),
    prd_start_dt   DATE,
    prd_end_dt     DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
create table bronze.crm_sales_details (
	sls_ord_num     VARCHAR(20),
    sls_prd_key     VARCHAR(30),
    sls_cust_id     INTEGER,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       NUMERIC(12, 2),
    sls_quantity    INTEGER,
    sls_price       NUMERIC(10, 2)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid     VARCHAR(20),
    bdate   DATE,
    gen     VARCHAR(10)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    id            VARCHAR(20),
    cat           VARCHAR(50),
    subcat        VARCHAR(50),
    maintenance   VARCHAR(10)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    cid     VARCHAR(20),
    cntry   VARCHAR(50)
);


