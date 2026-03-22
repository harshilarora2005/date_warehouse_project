DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN

    -- CRM TABLES
    TRUNCATE TABLE bronze_crm_cust_info;

    LOAD DATA INFILE '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze_crm_cust_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    TRUNCATE TABLE bronze_crm_prd_info;

    LOAD DATA INFILE '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze_crm_prd_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    TRUNCATE TABLE bronze_crm_sales_details;

    LOAD DATA INFILE '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze_crm_sales_details
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;


    -- ERP TABLES
    TRUNCATE TABLE bronze_erp_loc_a101;

    LOAD DATA INFILE '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/LOC_A101.csv'
    INTO TABLE bronze_erp_loc_a101
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    TRUNCATE TABLE bronze_erp_cust_az12;

    LOAD DATA INFILE '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/CUST_AZ12.csv'
    INTO TABLE bronze_erp_cust_az12
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

    TRUNCATE TABLE bronze_erp_px_cat_g1v2;

    LOAD DATA INFILE '/Users/harshilarora/Documents/date_warehouse_project/datasets/source_erp/PX_CAT_G1V2.csv'
    INTO TABLE bronze_erp_px_cat_g1v2
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;

END$$

DELIMITER ;