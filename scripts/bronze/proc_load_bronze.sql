-- ============================================================================
-- Procedure: bronze.load_bronze
-- Purpose:
--     This procedure loads raw data into the bronze layer of the data warehouse.
--     For each source CSV file, the corresponding target table is truncated,
--     and new data is loaded using the COPY command.
--     The process includes error handling, logging of steps via RAISE NOTICE,
--     and timing of each individual load operation as well as the total duration.
-- ============================================================================
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    total_start_time TIMESTAMP;
    total_end_time TIMESTAMP;
    total_duration INTERVAL;

    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    total_start_time := clock_timestamp();
    RAISE NOTICE 'Bronze loading process started.';

    -- Load bronze.crm_cust_info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Loading table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        FROM '/tmp/crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded bronze.crm_cust_info in %', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading bronze.crm_cust_info: %', SQLERRM;
    END;

    -- Load bronze.crm_prd_info
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Loading table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        FROM '/tmp/crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded bronze.crm_prd_info in %', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading bronze.crm_prd_info: %', SQLERRM;
    END;

    -- Load bronze.crm_sales_details
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Loading table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        FROM '/tmp/crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded bronze.crm_sales_details in %', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading bronze.crm_sales_details: %', SQLERRM;
    END;

    -- Load bronze.erp_cust_az12
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Loading table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12 (cid, bdate, gen)
        FROM '/tmp/erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded bronze.erp_cust_az12 in %', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading bronze.erp_cust_az12: %', SQLERRM;
    END;

    -- Load bronze.erp_px_cat_g1v2
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Loading table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        FROM '/tmp/erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded bronze.erp_px_cat_g1v2 in %', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading bronze.erp_px_cat_g1v2: %', SQLERRM;
    END;

    -- Load bronze.erp_loc_a101
    BEGIN
        start_time := clock_timestamp();
        RAISE NOTICE 'Loading table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101 (cid, cntry)
        FROM '/tmp/erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded bronze.erp_loc_a101 in %', duration;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading bronze.erp_loc_a101: %', SQLERRM;
    END;

    total_end_time := clock_timestamp();
    total_duration := total_end_time - total_start_time;
    RAISE NOTICE 'Bronze loading process completed in %', total_duration;
END;
$$;
