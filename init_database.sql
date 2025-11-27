/***********************************************************************************************
 Script Name   : Recreate DataWarehouse Database and Create Schema Layers
 Author        : Muhammad Tajammal Khalid
 Purpose       : 
    - Check if the "DataWarehouse" database exists
    - If exists → Force drop it safely (rollback any open transactions)
    - Create a fresh "DataWarehouse" database
    - Create bronze, silver, gold schema layers for Data Warehouse architecture
 Environment   : SQL Server
 Date Created  : <27/11/2025>
***********************************************************************************************/

-- Switch context to master database (required to drop/create another DB)
USE master;
GO

-- Check if the DataWarehouse DB already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force the database into single-user mode to avoid session lock issues
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database
    DROP DATABASE DataWarehouse;
END
GO

-- Create a new DataWarehouse database from scratch
CREATE DATABASE DataWarehouse;
GO

-- Change context to the newly created DataWarehouse database
USE DataWarehouse;
GO

-- Create Bronze schema (Raw data storage layer)
CREATE SCHEMA bronze;
GO

-- Create Silver schema (Cleansed/standardized data layer)
CREATE SCHEMA silver;
GO

-- Create Gold schema (Curated BI-ready data layer)
CREATE SCHEMA gold;
GO
