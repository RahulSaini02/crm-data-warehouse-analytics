/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
This script creates tables in the 'silver' schema, dropping existing tables 
if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key CHARACTER(50),
    cst_firstname CHARACTER(50),
    cst_lastname CHARACTER(50),
    cst_marital_status CHARACTER(50),
    cst_gndr CHARACTER(50),
    cst_create_date DATE,
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id CHARACTER(50),
    prd_key CHARACTER(50),
    prd_nm CHARACTER(50),
    prd_cost INT,
    prd_line CHARACTER(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT NOW()
);


CREATE TABLE silver.crm_sales_details (
    sls_ord_num CHARACTER(50),
    sls_prd_key CHARACTER(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date TIMESTAMP DEFAULT NOW()
);


CREATE TABLE silver.erp_loc_a101 (
    cid CHARACTER(50),
    cntry CHARACTER(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);


CREATE TABLE silver.erp_cust_az12 (
    cid CHARACTER(50),
    bdate DATE,
    gen CHARACTER(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

CREATE TABLE silver.erp_px_cat_g1v2 (
    id CHARACTER(50),
    cat CHARACTER(50),
    subcat CHARACTER(50),
    maintenance CHARACTER(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);