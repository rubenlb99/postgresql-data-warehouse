-- customer dim

CREATE OR REPLACE VIEW gold.dim_customer AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS firstname,
    ci.cst_lastname AS lastname,
    cloc.cntry AS country,
    CASE 
        WHEN ci.cst_gndr != 'n/a'
        THEN ci.cst_gndr
        ELSE COALESCE(caz.gen, 'n/a')
    END AS gender,
    caz.bdate AS birthdate,
    ci.cst_marital_status AS marital_status,
    ci.cst_create_date AS created_date
FROM silver.crm_cust_info ci
JOIN silver.erp_cust_az12 caz
    ON ci.cst_key = caz.cid
JOIN silver.erp_loc_a101 cloc
    ON ci.cst_key = cloc.cid;

