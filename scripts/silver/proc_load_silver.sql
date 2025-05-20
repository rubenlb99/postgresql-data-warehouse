/*
===============================================================================
Procedure: load_silver
===============================================================================
Purpose:
    This stored procedure loads cleaned and transformed data from the 'bronze' 
    schema into the corresponding tables in the 'silver' schema.
    It truncates and refreshes all silver tables with validated data.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Load crm_cust_info
    RAISE NOTICE 'Loading data into silver.crm_cust_info...';
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        TRIM(cst_key),
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id 
                   ORDER BY cst_create_date DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
    ) AS t
    WHERE flag_last = 1;

    -- Load crm_prd_info
    RAISE NOTICE 'Loading data into silver.crm_prd_info...';
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 0, 6), '-', '_'),
        SUBSTRING(prd_key, 7, LENGTH(prd_key)),
        TRIM(prd_nm),
        COALESCE(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'M' THEN 'Mountain'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        prd_start_dt,
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE
    FROM bronze.crm_prd_info;

    -- Load crm_sales_details
    RAISE NOTICE 'Loading data into silver.crm_sales_details...';
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        CASE
            WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN ABS(sls_price) * sls_quantity
            ELSE sls_sales
        END,
        sls_quantity,
        CASE
            WHEN sls_price < 0 THEN ABS(sls_price)
            WHEN sls_price = 0 OR sls_price IS NULL THEN ABS(sls_sales) / sls_quantity
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    -- Load erp_cust_az12
    RAISE NOTICE 'Loading data into silver.erp_cust_az12...';
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END,
        CASE
            WHEN gen = 'Female' OR UPPER(TRIM(gen)) = 'F' THEN 'Female'
            WHEN gen = 'Male' OR UPPER(TRIM(gen)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    -- Load erp_loc_a101
    RAISE NOTICE 'Loading data into silver.erp_loc_a101...';
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN UPPER(cntry) IN ('UNITED STATES', 'USA', 'US') THEN 'United States'
            WHEN UPPER(cntry) IN ('DE', 'GERMANY') THEN 'Germany'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    -- Load erp_px_cat_g1v2
    RAISE NOTICE 'Loading data into silver.erp_px_cat_g1v2...';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    RAISE NOTICE 'Silver layer load completed successfully.';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred during silver layer load: %', SQLERRM;
        -- Optional: re-raise error or handle it accordingly
END;
$$;
