-- =========================================
-- Quality Checks for Bronze Layer
-- Run after bronze load
-- =========================================


-- =========================================
-- bronze.crm_cust_info
-- =========================================

-- Null or duplicate customer IDs
SELECT
    cst_id,
    COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check raw string spaces
SELECT
    cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Distinct marital status values
SELECT DISTINCT
    cst_marital_status
FROM bronze.crm_cust_info;


-- =========================================
-- bronze.crm_prd_info
-- =========================================

-- Null or duplicate product IDs
SELECT
    prd_id,
    COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Negative costs
SELECT
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Product line values
SELECT DISTINCT
    prd_line
FROM bronze.crm_prd_info;


-- =========================================
-- bronze.crm_sales_details
-- =========================================

-- Invalid due date integers
SELECT
    sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
   OR LENGTH(sls_due_dt::TEXT) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-- Null customer/product references
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL
   OR sls_prd_key IS NULL;

-- Invalid sales values
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;


-- =========================================
-- bronze.erp_cust_az12
-- =========================================

-- Invalid birthdates
SELECT DISTINCT
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < DATE '1924-01-01'
   OR bdate > CURRENT_DATE;

-- Gender values
SELECT DISTINCT
    gen
FROM bronze.erp_cust_az12;


-- =========================================
-- bronze.erp_loc_a101
-- =========================================

-- Country values
SELECT DISTINCT
    cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


-- =========================================
-- bronze.erp_px_cat_g1v2
-- =========================================

-- Trim checks
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Maintenance values
SELECT DISTINCT
    maintenance
FROM bronze.erp_px_cat_g1v2;