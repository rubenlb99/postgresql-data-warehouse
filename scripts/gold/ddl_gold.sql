CREATE OR REPLACE PROCEDURE gold.load_gold()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Creating or replacing view: gold.dim_customer';
    EXECUTE $customer_dim$
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
    $customer_dim$;

    RAISE NOTICE 'Creating or replacing view: gold.dim_products';
    EXECUTE $product_dim$
        CREATE OR REPLACE VIEW gold.dim_products AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
            pi.prd_id AS product_id,
            pi.prd_key AS product_number,
            pi.prd_nm AS product_name,
            pi.cat_id AS category_id,
            cat.cat AS category,
            cat.subcat AS subcategory,
            cat.maintenance AS maintenance,
            pi.prd_cost AS cost,
            pi.prd_line AS product_line,
            pi.prd_start_dt AS start_date
        FROM silver.crm_prd_info pi
        JOIN silver.erp_px_cat_g1v2 cat
            ON pi.cat_id = cat.id
        WHERE pi.prd_end_dt IS NULL;
    $product_dim$;

    RAISE NOTICE 'Creating or replacing view: gold.fact_sales';
    EXECUTE $fact_sales$
        CREATE OR REPLACE VIEW gold.fact_sales AS
        SELECT
            s.sls_ord_num AS order_number,
            c.customer_key AS customer_key,
            p.product_key AS product_key,
            s.sls_order_dt AS order_date,
            s.sls_ship_dt AS shipping_date,
            s.sls_due_dt AS due_date,
            s.sls_quantity AS quantity,
            s.sls_price AS price,
            s.sls_sales AS total_amount
        FROM silver.crm_sales_details s
        JOIN gold.dim_products p
            ON s.sls_prd_key = p.product_number
        JOIN gold.dim_customer c
            ON s.sls_cust_id = c.customer_id;
    $fact_sales$;

    RAISE NOTICE 'All gold views created or replaced successfully.';
END;
$$;
