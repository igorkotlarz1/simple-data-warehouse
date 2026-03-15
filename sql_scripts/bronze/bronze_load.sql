
/*
	Creating Stored Procedure to load raw CSV data into the Bronze layer tables.
	At first the tables are truncated before loading the data. Then the CSV data is loaded using BULK INSERT statements.
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		-- LOADING CRM DATA
		-- CUST_INFO table
		TRUNCATE TABLE bronze.crm_cust_info;

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\igor\Desktop\GitHub projects\simple-data-warehouse\data\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- PROD_INFO table
		TRUNCATE TABLE bronze.crm_prd_info;

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\igor\Desktop\GitHub projects\simple-data-warehouse\data\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- SALES_DETAILS table
		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\igor\Desktop\GitHub projects\simple-data-warehouse\data\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- LOADING ERP DATA

		-- CUST_AZ12 table
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\igor\Desktop\GitHub projects\simple-data-warehouse\data\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- LOC_A101 table
		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\igor\Desktop\GitHub projects\simple-data-warehouse\data\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- PX_CAT_G1V2 table
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\igor\Desktop\GitHub projects\simple-data-warehouse\data\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	END TRY

	BEGIN CATCH
		PRINT 'ERROR WHILE LOADING THE BRONZE LAYER TABLES'
		PRINT 'Error message: ' + ERROR_MESSAGE()
		PRINT 'Error code: ' + CAST(ERROR_NUMBER() AS VARCHAR)
	END CATCH
END