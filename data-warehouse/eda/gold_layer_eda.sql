/*
======================================================================================
Performing data quality checks and transformation logics to write queries 
required for creating views for fact and dim tables from silver layer to gold layer.
======================================================================================
*/

/*
============================================================================
Data Quality check for Customer Info
============================================================================
*/

SELECT 
  ci.cst_id,
  ci.cst_key,
  ci.cst_firstname,
  ci.cst_lastname,
  ci.cst_marital_status,
  ci.cst_gndr,
  ci.cst_create_date,
  ca.bdate,
  ca.gen,
  la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid;

-- Checking if no duplicates are formed
SELECT t.cst_key, count(*)
from (
  SELECT ci.cst_id, ci.cst_key, ci.cst_firstname, ci.cst_lastname, ci.cst_marital_status, ci.cst_gndr, ci.cst_create_date, ca.bdate, ca.gen, la.cntry
FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
)t group by t.cst_key
having count(*) > 1;

SELECT DISTINCT
  ci.cst_gndr,
  ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid;

-- Considering CRM data is the correct data implementing Data Integration
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
    ELSE COALESCE(ca.gen, 'N/A')
    END as gender
FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;

SELECT 
  ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
  ci.cst_id as customer_id,
  ci.cst_key as customer_number,
  ci.cst_firstname as first_name,
  ci.cst_lastname as last_name,
  la.cntry as country,
  ci.cst_marital_status as marital_status,
  CASE
    WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
    ELSE COALESCE(ca.gen, 'N/A')
  END AS gender,
  ca.bdate as birthdate,
  ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid;


SELECT
    ROW_NUMBER() OVER (
        ORDER BY pn.prd_start_dt, pn.prd_key
    ) AS product_key, -- Surrogate key
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS COST,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NULL;

SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;