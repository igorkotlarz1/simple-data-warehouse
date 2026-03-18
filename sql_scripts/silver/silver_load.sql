/*
	Creating Stored Procedure to clean, transform and then load the data from the Bronze into the Silver layer.
	At first the tables are truncated. 
	Then the ETL (Extract, Transform, Load) process is being performed to load transformed data into the silver schema tables.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		-- ========================== 
		-- CRM TABLES

		-- transforming and loading CRM_CUST_INFO table
		TRUNCATE TABLE silver.crm_cust_info
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT 
			cst_id, 
			cst_key, 
			-- removing trailing spaces from first and last name
			TRIM(cst_firstname) AS cst_firstname, 
			TRIM(cst_lastname) AS cst_lastname,
			-- standardizing the marital status values
			CASE UPPER(TRIM(cst_marital_status)) 
				WHEN 'M' THEN 'Married'
				WHEN 'S' THEN 'Single'
				ELSE 'Unknown' END AS cst_marital_status,

			-- standardizing the gender values 
			CASE UPPER(TRIM(cst_gndr)) 
				WHEN 'M' THEN 'Male'
				WHEN 'F' THEN 'Female'
				ELSE 'Unknown' END AS cst_gndr,
				cst_create_date
		FROM (
			-- removing duplicates by selecting a record with the most recent creation date 
			SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_created 
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) AS s
		WHERE s.last_created = 1;

		-- ==========================
		-- transforming and loading CRM_PRD_INFO table
		TRUNCATE TABLE silver.crm_prd_info
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id, 
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
			prd_id,
			-- extracting category id and product key from the prd_key column
			REPLACE(LEFT(prd_key, 5),'-','_') AS cat_id, -- new column
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,

			-- mapping product line abbreviations to their meanings - labels provided by the Author of the Dataset
			CASE UPPER(TRIM(prd_line)) 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'Unknown' 
			END AS prd_line,
			prd_start_dt,

			/* addressing issues with invalid end_dates - if end_date < start_date we will replace it with the start_date - 1 day of the following
			record of the same product key (using LEAD window function) */
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt
		FROM bronze.crm_prd_info;

		-- ==========================
		-- transforming and loading CRM_SALES_DEATILS table
		TRUNCATE TABLE silver.crm_sales_details
		INSERT INTO silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt, 
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			-- casting date columns formatted as INTs into DATE format 
			CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
			CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
			CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)  END AS sls_due_dt,

			-- resolving issues with inconsistent sales values (not matching the rule: sales = price * quantity )
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN ABS(sls_price) * sls_quantity
				ELSE sls_sales 
			END AS sls_sales,

			sls_quantity,

			-- resolving issues with missing and negative values in the sls_price column
			CASE WHEN sls_price IS NULL AND sls_sales IS NOT NULL
				THEN sls_sales / NULLIF(sls_quantity,0)
				WHEN sls_price < 0 THEN ABS(sls_price) 
				ELSE sls_price 
			END AS sls_price
		FROM bronze.crm_sales_details;

		-- ========================== 
		-- ERP TABLES

		-- transforming and loading ERP_CUST_AZ12 table
		TRUNCATE TABLE silver.erp_cust_az12
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

		SELECT 
			-- removing the 'NAS' prefix 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
				ELSE cid END AS cid,
			-- catching the anomalies of birth dates from the future
			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate END AS bdate,
			-- standardizing gender values
			CASE WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
				WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male' 
				ELSE 'Unknown' END AS gen
		FROM bronze.erp_cust_az12;

		-- ==========================
		-- transforming and loading ERP_LOC_A101 table
		TRUNCATE TABLE silver.erp_loc_a101
		INSERT INTO silver.erp_loc_a101(cid,cntry)

		SELECT 
			-- adjusting cid values to match the crm_cust_info.cid key
			REPLACE(cid, '-','') AS cid,
			-- standardizing country name values into readable format
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL then 'Unknown'
				ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101;

		-- ==========================
		-- loading ERP_LOC_A101 table - nothing to transform
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		INSERT INTO silver.erp_px_cat_g1v2(
			id, cat, subcat, maintenance
		)
		SELECT * FROM bronze.erp_px_cat_g1v2;

	END TRY
	BEGIN CATCH
		PRINT 'ERROR WHILE LOADING THE SILVER LAYER TABLES'
		PRINT 'Error message: ' + ERROR_MESSAGE()
		PRINT 'Error code: ' + CAST(ERROR_NUMBER() AS VARCHAR)
	END CATCH
END
