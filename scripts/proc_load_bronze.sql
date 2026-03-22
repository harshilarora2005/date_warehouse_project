CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Bronze Layer Started';
    RAISE NOTICE '========================================';

    -- CRM CUSTOMER INFO
    RAISE NOTICE 'Loading crm_cust_info...';
    TRUNCATE TABLE bronze.crm_cust_info;

    COPY bronze.crm_cust_info
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;

    RAISE NOTICE 'crm_cust_info loaded successfully';


    -- CRM PRODUCT INFO
    RAISE NOTICE 'Loading crm_prd_info...';
    TRUNCATE TABLE bronze.crm_prd_info;

    COPY bronze.crm_prd_info
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;

    RAISE NOTICE 'crm_prd_info loaded successfully';


    -- CRM SALES DETAILS
    RAISE NOTICE 'Loading crm_sales_details...';
    TRUNCATE TABLE bronze.crm_sales_details;

    COPY bronze.crm_sales_details
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;

    RAISE NOTICE 'crm_sales_details loaded successfully';


    -- ERP LOCATION
    RAISE NOTICE 'Loading erp_loc_a101...';
    TRUNCATE TABLE bronze.erp_loc_a101;

    COPY bronze.erp_loc_a101
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/LOC_A101.csv'
    DELIMITER ','
    CSV HEADER;

    RAISE NOTICE 'erp_loc_a101 loaded successfully';


    -- ERP CUSTOMER
    RAISE NOTICE 'Loading erp_cust_az12...';
    TRUNCATE TABLE bronze.erp_cust_az12;

    COPY bronze.erp_cust_az12
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;

    RAISE NOTICE 'erp_cust_az12 loaded successfully';


    -- ERP PRODUCT CATEGORY
    RAISE NOTICE 'Loading erp_px_cat_g1v2...';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/PX_CAT_G1V2.csv'
    DELIMITER ','
    CSV HEADER;

    RAISE NOTICE 'erp_px_cat_g1v2 loaded successfully';

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Bronze Layer Loading Completed';
    RAISE NOTICE '========================================';

END;
$$;

CALL bronze.load_bronze();