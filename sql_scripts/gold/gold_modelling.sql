/*
	Script for the final step of the project - data modelling in the Gold layer. 
	Data from the Silver layer is modeled into a Star Schema (fact and dimension tables) - where sale is the fact,
	and customers and products are the dimensions providing additional context to the measures stored in the fact table (in this case view).
	
	Eeach view provides a dataset ready for business analysis.
*/

-- ================================
-- CUSTOMER DIMENSION 

CREATE OR ALTER VIEW gold.dim_customer AS
SELECT
-- creating a surrogate key 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id, 
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,	
	-- CRM was established to be more significant source of information than ERP
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr  
		ELSE COALESCE(ca.gen, 'Unknown') END AS gender,
	ci.cst_marital_status AS marital_status,
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date
	
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl ON ci.cst_key = cl.cid;

GO

-- ================================
-- PRODUCT DIMENSION 

CREATE OR ALTER VIEW gold.dim_product AS
SELECT 
	-- creating a surrogate key 
	ROW_NUMBER() OVER (ORDER BY pin.prd_start_dt, pin.prd_id) AS product_key,
	pin.prd_id AS product_id, 
	pin.prd_key AS product_number, 
	pin.prd_nm AS product_name, 
	pin.cat_id AS category_id, 
	pc.cat AS category, 
	pc.subcat AS subcategory,
	pc.maintenance,  
	pin.prd_cost AS cost, 
	pin.prd_line AS product_line, 
	pin.prd_start_dt AS product_start_date
FROM silver.crm_prd_info AS pin
LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pin.cat_id = pc.id
WHERE pin.prd_end_dt IS NULL;

GO

-- ================================
-- SALES - FACT TABLE

CREATE OR ALTER VIEW gold.fact_sale AS
SELECT
	sd.sls_ord_num AS order_number, 
	pr.product_key ,
	c.customer_key,
	sd.sls_order_dt AS order_date, 
	sd.sls_ship_dt AS shipment_date, 
	sd.sls_due_dt AS due_date, 
	sd.sls_sales AS total_amount, 
	sd.sls_quantity AS quantity, 
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_product AS pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer AS c ON sd.sls_cust_id = c.customer_id;