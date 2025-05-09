/*
======================================================================================
Performing data quality checks and transformation logics to write queries 
required for Data transformation of raw data from bronze layer to silver layer.
======================================================================================
*/

/*
============================================================================
Data Quality check for `bronze.crm_cust_info`
============================================================================
*/

-- Check for Nulls or Duplicates in  Primary Key
-- Exception: No Result
SELECT cst_id, count(*)
FROM bronze.crm_cust_info
GROUP BY
    cst_id
HAVING
    count(*) > 1
    OR cst_id IS NULL;

-- To get the latest date of the customer info
SELECT *
FROM (
        SELECT *, ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM bronze.crm_cust_info
    ) t
WHERE
    flag_last = 1;

-- Check for unwanted spaces
-- FIRSTNAME
SELECT COUNT(cst_firstname)
FROM bronze.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname);

-- LASTNAME
SELECT COUNT(cst_lastname)
FROM bronze.crm_cust_info
WHERE
    cst_lastname != TRIM(cst_lastname);

-- MARTIAL STATUS
SELECT COUNT(cst_marital_status)
FROM bronze.crm_cust_info
WHERE
    cst_marital_status != TRIM(cst_marital_status);

-- GENDER
SELECT COUNT(cst_gndr)
FROM bronze.crm_cust_info
WHERE
    cst_gndr != TRIM(cst_gndr);

-- Check for data Standardization & Consistency
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

/*
========================================================================================
Final query for `silver.crm_cust_info` to load data from bonze layer to silver layer.
========================================================================================
*/

SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END AS cst_gndr,
    cst_create_date
FROM (
        SELECT *, ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
    ) t
WHERE
    flag_last = 1;

/*
============================================================================
Data Quality check for `bronze.crm_prd_info`
============================================================================
*/

-- Check for Duplicates or Nulls
SELECT prd_id, count(*)
FROM bronze.crm_prd_info
GROUP BY
    prd_id
HAVING
    count(*) > 1
    OR prd_id IS NULL;

-- Check for unwanted spaces
-- Product Key
SELECT COUNT(prd_key)
FROM bronze.crm_prd_info
WHERE
    prd_key != TRIM(prd_key);

-- Product Name
SELECT COUNT(prd_nm)
FROM bronze.crm_prd_info
WHERE
    prd_nm != TRIM(prd_nm);

-- Check for negative cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE
    prd_cost <= 0
    OR prd_cost IS NULL;

-- Check for data Standardization & Consistency
SELECT DISTINCT prd_line FROM bronze.crm_prd_info;

-- Check for invalid date orders
SELECT * FROM bronze.crm_prd_info WHERE prd_end_dt < prd_start_dt;

-- We have records where the end date is less than the start date
-- To fix this we can modify the end date of a product key to have the end date less than the next start date and for the latest record we can keep it as `NULL`
SELECT
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD(prd_start_dt, 1) OVER (
            PARTITION BY
                prd_key
            ORDER BY prd_start_dt
        ) - INTERVAL '1 day' AS DATE
    ) AS prd_end_dt -- Calculate end date as one date previous to next start date
FROM bronze.crm_prd_info;

/*
======================================================================================
Final query for `silver.crm_prd_info` to load data from bonze layer to silver layer.
======================================================================================
*/
SELECT
  prd_id,
  REPLACE(
      SUBSTRING(prd_key, 1, 5),
      '-',
      '_'
  ) AS cat_id, -- EXTRACT catgory id  to join with "bronze.erp_px_cat_g1v2"
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- EXTRACT product key to join with "bronze.crm_sales_details"
  prd_nm,
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE UPPER(TRIM(prd_line))
      WHEN 'M' THEN 'Mountain'
      WHEN 'R' THEN 'Road'
      WHEN 'S' THEN 'Other Sales'
      WHEN 'T' THEN 'Touring'
      ELSE 'N/A'
  END AS prd_line -- Map to get desrciptive values: Data normalization,
  CAST(prd_start_dt AS DATE) AS prd_start_dt,
  CAST(LEAD(prd_start_dt, 1) OVER(
    PARTITION BY prd_key
    ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_dt -- Calculate end date as one date previous to next start date
FROM bronze.crm_prd_info;

/*
============================================================================
Data Quality check for `bronze.crm_sales_details`
============================================================================
*/

SELECT * FROM bronze.crm_sales_details;

-- Check for unwanted spaces
-- Exception: No Result
SELECT *
FROM bronze.crm_sales_details
WHERE
    sls_ord_num != TRIM(sls_ord_num);

/*
CHECK FOR Invalid Date 
  - Converting order_dt, ship_dt, due_dt
  - FROM integer TO Date - CHECK IF it IS ANY VALUES less than OR equal TO zero, or does not match the len 8 and date is beyond the boundry
  - IF ANY CONVERT them TO NULL - CONVERT integer TO VARCHAR THEN CAST TO date
*/
SELECT 
  sls_ord_num, 
  sls_prd_key,
  sls_cust_id,
  CASE 
    WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE ) 
  END AS sls_order_dt,
  CASE 
    WHEN sls_ship_dt = 0 OR LENGTH (CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
    ELSE CAST( CAST(sls_ship_dt AS VARCHAR) AS DATE ) 
  END AS sls_ship_dt,
  CASE 
    WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
    ELSE CAST( CAST(sls_due_dt AS VARCHAR) AS DATE ) 
  END AS sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
FROM bronze.crm_sales_details
where sls_ord_num = 'SO43697';

/* Check for Invalid order dates*/
SELECT * FROM
bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

/*
Chcking Business Rules
Sales = Quantity * Price

   - If Sales are -ve, zero, or null derive it using quantity and price.
   - If price is zero or null, calculate it using sales and quantity.
   - If price is negative, convert it to a positive value.
*/

select DISTINCT
  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END as sls_sales,
  sls_quantity,
  CASE 
    WHEN sls_price <= 0 or sls_price IS NULL THEN  sls_sales / NULLIF(sls_quantity, 0)
    ELSE  ABS(sls_price)
  END as sls_price

From bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales is NULL or sls_quantity is NULL or sls_price is NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

/*
===========================================================================================
Final query for `silver.crm_sales_details` to load data from bonze layer to silver layer.
===========================================================================================
*/
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
        WHEN sls_order_dt = 0
        OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
        ELSE CAST(
            CAST(sls_order_dt AS VARCHAR) AS DATE
        )
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt = 0
        OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
        ELSE CAST(
            CAST(sls_ship_dt AS VARCHAR) AS DATE
        )
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt = 0
        OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
        ELSE CAST(
            CAST(sls_due_dt AS VARCHAR) AS DATE
        )
    END AS sls_due_dt,
    CASE
        WHEN sls_sales IS NULL
        OR sls_sales <= 0
        OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
    sls_quantity,
    CASE
        WHEN sls_price <= 0
        OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE ABS(sls_price)
    END AS sls_price -- Derive price if original value is invalid
  FROM bronze.crm_sales_details;

/*
============================================================================
Data Quality check for `bronze.erp_cust_az12`
============================================================================
*/

SELECT * FROM bronze.erp_cust_az12;

SELECT * FROM silver.crm_cust_info;

SELECT * FROM bronze.erp_cust_az12 WHERE CID LIKE '%AW00011000%'; -- We have extra characters `NAS` in the cid that we can remove as it does not match with the cst_key in crm_cust_info

-- Remove 'NAS' from cid
SELECT 
  CASE 
    WHEN cid like 'NAS%' THEN  substring(cid, 4, length(cid))
    ELSE cid
  END AS cid,
  bdate,
  gen
FROM bronze.erp_cust_az12;

-- Check for bdate which or in future or very old
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN substring(cid, 4, length(cid))
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > NOW();

-- Lets change the future dates to NULL
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN substring(cid, 4, length(cid))
        ELSE cid
    END AS cid,
    CASE 
      WHEN bdate > NOW() THEN NULL
      ELSE bdate
    END AS bdate,
    gen
FROM bronze.erp_cust_az12;

-- Check for Gender
SELECT DISTINCT gen,
CASE
  WHEN upper(TRIM(gen)) IN ('F', 'FEMALE') THEN  'Female'
  WHEN upper(TRIM(gen)) IN ('M', 'MALE') THEN  'Male'
  ELSE 'N/A'
END
FROM bronze.erp_cust_az12; 

/*
===========================================================================================
Final query for `silver.erp_cust_az12` to load data from bonze layer to silver layer.
===========================================================================================
*/
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN substring(cid, 4, length(cid))
        ELSE cid
    END AS cid,
    CASE
        WHEN bdate > NOW() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE
      WHEN upper(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
      WHEN upper(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
      ELSE 'N/A'
    END as gen
FROM bronze.erp_cust_az12;


/*
============================================================================
Data Quality check for `bronze.erp_loc_a101`
============================================================================
*/

SELECT * FROM bronze.erp_loc_a101;

/*
CHECK FOR unwated space
  - cid
  - cntry
*/
SELECT cid FROM bronze.erp_loc_a101 WHERE cid != TRIM(cid);
SELECT cntry FROM bronze.erp_loc_a101 WHERE cntry != TRIM(cntry);


-- Check for valid cid
SELECT DISTINCT cid FROM bronze.erp_loc_a101 WHERE cid like '%AW00011000%'; -- We have a '-' in between the cid remove it to make the data valid.

SELECT 
REPLACE(cid, '-', '') as cid
FROM bronze.erp_loc_a101;

-- Check cntry column
SELECT DISTINCT cntry FROM bronze.erp_loc_a101;

/*
In cntry we can see that the data is not normalized, we have abbv, and NULL
  - US
  - United States
  - USA
  - DE
*/

SELECT DISTINCT cntry, 
CASE 
  WHEN UPPER(trim(cntry)) = 'DE' THEN  'Germany'
  WHEN UPPER(trim(cntry)) in ('US', 'USA') THEN  'United States'
  WHEN UPPER(trim(cntry)) = '' OR cntry IS NULL THEN 'N/A'
  ELSE cntry
END
FROM bronze.erp_loc_a101;

/*
===========================================================================================
Final query for `silver.erp_loc_a101` to load data from bonze layer to silver layer.
===========================================================================================
*/
SELECT 
  REPLACE(cid, '-', '') AS cid,
  CASE
      WHEN UPPER(trim(cntry)) = 'DE' THEN 'Germany'
      WHEN UPPER(trim(cntry)) IN ('US', 'USA') THEN 'United States'
      WHEN UPPER(trim(cntry)) = ''
      OR cntry IS NULL THEN 'N/A'
      ELSE cntry
  END AS cntry
FROM bronze.erp_loc_a101;


/*
============================================================================
Data Quality check for `bronze.erp_px_cat_g1v2`
============================================================================
*/

SELECT * FROM bronze.erp_px_cat_g1v2;

/*
CHECK FOR unwated space

  - cat
  - subcat
  - maintenance
*/
SELECT * FROM bronze.erp_px_cat_g1v2 WHERE 
cat != TRIM(cat) OR 
subcat != TRIM(subcat) OR
maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;

-- There are no unwanted spaces or Null values

/*
===========================================================================================
Final query for `silver.erp_loc_a101` to load data from bonze layer to silver layer.
===========================================================================================
*/
SELECT * FROM bronze.erp_px_cat_g1v2;