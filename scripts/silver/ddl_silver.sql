DROP TABLE IF EXISTS silver.crm_cust_info;
create table silver.crm_cust_info (
	cst_id              INTEGER,
    cst_key             VARCHAR(20),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  CHAR(1),         -- E.g. M, S
    cst_gndr            CHAR(1),         -- E.g. M, F
    cst_create_date     DATE,
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.crm_prd_info;
create table silver.crm_prd_info (
	prd_id         INTEGER,
    prd_key        VARCHAR(30),
    prd_nm         VARCHAR(100),
    prd_cost       NUMERIC(10,2),    
    prd_line       VARCHAR(10),
    prd_start_dt   DATE,
    prd_end_dt     DATE,
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.crm_sales_details;
create table silver.crm_sales_details (
	sls_ord_num     VARCHAR(20),
    sls_prd_key     VARCHAR(30),
    sls_cust_id     INTEGER,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       NUMERIC(12, 2),
    sls_quantity    INTEGER,
    sls_price       NUMERIC(10, 2),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid     VARCHAR(20),
    bdate   DATE,
    gen     VARCHAR(10),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id            VARCHAR(20),
    cat           VARCHAR(50),
    subcat        VARCHAR(50),
    maintenance   VARCHAR(10),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid     VARCHAR(20),
    cntry   VARCHAR(50),
	dwh_create_date 	DATE DEFAULT CURRENT_DATE
);

