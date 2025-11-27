/***********************************************************************************************
 Script Name   : Recreate DataWarehouse Database and Create Schema Layers
 Author        : Muhammad Tajammal Khalid
PURPOSE:
    - Drop existing DataWarehouse database (if it exists)
    - Create a fresh DataWarehouse database
    - Create standardized schema layers: bronze, silver, gold
    - Used for Data Warehouse architecture setup

 ⚠️ WARNING:
    - This script will permanently DROP the existing DataWarehouse database.
    - All data will be lost and cannot be recovered!
    - Ensure you have proper backups before running this script in Production.
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
