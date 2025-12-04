/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Purpose:
    This script creates the views for the Gold layer of the data warehouse.
    The Gold layer represents the finalized dimension and fact structures 
    (Star Schema).

    Each view applies transformations and combines data from the Silver layer
    to produce clean, enriched, and analytics-ready datasets.

Usage:
    - These views can be queried directly for reporting and analytical workloads.
===============================================================================
*/



IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT 
ROW_NUMBER () OVER (ORDER BY ci.cst_id ) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cts_lastname AS last_name,
la.cntry AS country,
ci.cts_material_status AS marital_status,
CASE WHEN ci.cst_gndr !='n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'n/a')
END AS gender,
ca.bdate AS birthday,
ci.cst_create_date AS create_date
From silver.crm_cust_info AS ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 AS la 
ON ci.cst_key= ca.cid
GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS 
SELECT 
ROw_NUMBER() OVER (ORDER BY pn.prdt_start_dt,pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key    AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS catagory_id,
pc.cat AS category,
pc.subcat AS subcatagory,
pc.maintenance,
pn.prt_cost AS cost,
pn.prd_line AS product_line,
pn.prdt_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_cat_g1v2 AS pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
select 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sla_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sla_quantity AS quanity,
sd.sla_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id
select * from gold.fact_sales 
GO
