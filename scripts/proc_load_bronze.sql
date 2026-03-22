CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Bronze Layer Started';
    RAISE NOTICE '========================================';

    -- CRM CUSTOMER INFO
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading crm_cust_info...';

    TRUNCATE TABLE bronze.crm_cust_info;

    COPY bronze.crm_cust_info
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();

    RAISE NOTICE 'crm_cust_info loaded successfully';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- CRM PRODUCT INFO
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading crm_prd_info...';

    TRUNCATE TABLE bronze.crm_prd_info;

    COPY bronze.crm_prd_info
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();

    RAISE NOTICE 'crm_prd_info loaded successfully';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- CRM SALES DETAILS
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading crm_sales_details...';

    TRUNCATE TABLE bronze.crm_sales_details;

    COPY bronze.crm_sales_details
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();

    RAISE NOTICE 'crm_sales_details loaded successfully';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- ERP LOCATION
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading erp_loc_a101...';

    TRUNCATE TABLE bronze.erp_loc_a101;

    COPY bronze.erp_loc_a101
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/LOC_A101.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();

    RAISE NOTICE 'erp_loc_a101 loaded successfully';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- ERP CUSTOMER
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading erp_cust_az12...';

    TRUNCATE TABLE bronze.erp_cust_az12;

    COPY bronze.erp_cust_az12
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();

    RAISE NOTICE 'erp_cust_az12 loaded successfully';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    -- ERP PRODUCT CATEGORY
    start_time := clock_timestamp();
    RAISE NOTICE 'Loading erp_px_cat_g1v2...';

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/PX_CAT_G1V2.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();

    RAISE NOTICE 'erp_px_cat_g1v2 loaded successfully';
    RAISE NOTICE 'Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    batch_end_time := clock_timestamp();

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Bronze Layer Loading Completed';
    RAISE NOTICE 'Total Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING BRONZE LOAD';
        RAISE NOTICE 'Error: %', SQLERRM;
        RAISE NOTICE '========================================';
END;
$$;

CALL bronze.load_bronze();