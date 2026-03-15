
/*
	Creating the DataWarehouse database and schemas for all layers: Bronze, Silver, Gold. 
*/

USE MASTER;
GO

-- Creating the database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Creating schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO