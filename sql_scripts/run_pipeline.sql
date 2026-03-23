/*
	Creating Master Stored Procedure. 
	This procedure orchestrates the ETL process by executing 'load_bronze' and 'load_silver' procedures sequentially (extracting raw data into Bronze layer, then data cleansing and transformations in Silver layer).
	It logs execution times for each step.
*/

CREATE OR ALTER PROCEDURE dbo.run_pipeline AS
BEGIN
	DECLARE @start DATETIME, @end DATETIME, @batch_start DATETIME, @batch_end DATETIME;

	BEGIN TRY
		PRINT 'RUNNING THE ETL PIPELINE'
		PRINT '==============================='
		
		SET @batch_start = GETDATE()

		PRINT '1. Loading BRONZE LAYER...'

		SET @start = GETDATE()
		EXEC bronze.load_bronze
		SET @end = GETDATE()

		PRINT 'Completed successfully in ' + CAST(DATEDIFF(SECOND, @start, @end) AS VARCHAR) + ' second(s).'
		PRINT '==============================='

		PRINT '2. Loading SILVER LAYER...'

		SET @start = GETDATE()
		EXEC silver.load_silver
		SET @end = GETDATE()

		SET @batch_end = GETDATE()
		PRINT 'Completed successfully in ' + CAST(DATEDIFF(SECOND, @start, @end) AS VARCHAR) + ' second(s).'
		PRINT '==============================='

		PRINT 'All completed in ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS VARCHAR) + ' second(s).'
	END TRY 
	BEGIN CATCH
		PRINT '==============================='
		PRINT 'ERROR WHILE EXECUTING THE ETL PIPELINE'
		PRINT 'Error message: ' + ERROR_MESSAGE()
		PRINT 'Error code: ' + CAST(ERROR_NUMBER() AS VARCHAR)
	END CATCH
END


EXEC dbo.run_pipeline