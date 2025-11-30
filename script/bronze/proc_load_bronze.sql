/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source → Bronze)
===============================================================================
Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV
    files. It carries out the following tasks:
    - Truncates the existing bronze tables before loading new data.
    - Utilizes BULK INSERT operations to import CSV file data into bronze tables.

Parameters:
    None.
    This procedure does not accept input parameters or return values.

Usage:
    EXEC bronze.load_bronze;
===============================================================================
*/


USE DataWarehouse;
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
      
    DECLARE @start_time DATETIME , @end_time DATETIME ,@bach_start_time DATETIME , @bach_end_time DATETIME;
    BEGIN TRY
            SET @bach_start_time = GETDATE();
            PRINT '===========================================';
            PRINT 'loading Bronze layer';
            PRINT '===========================================';


            PRINT '--------------------------------------------';
            PRINT 'loading CRM Tables';
            PRINT '--------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>Truncate Table bronze.crm_cust_info';
            PRINT '>Insert Data Into bronze.crm_cust_info';
            TRUNCATE TABLE bronze.crm_cust_info;
            BULK INSERT bronze.crm_cust_info
            FROM 'C:\Users\Tajammal khalid\Downloads\source_crm\cust_info.csv'
            WITH
            (
                FIRSTROW =2,
                FIELDTERMINATOR=',',
                TABLOCK
             );
            SET @end_time = GETDATE();
            PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';

            SET @start_time = GETDATE();
            PRINT '>Truncate Table bronze.crm_prd_info';
            PRINT '>Insert Data Into bronze.crm_prd_info';
             TRUNCATE TABLE bronze.crm_prd_info;
            BULK INSERT bronze.crm_prd_info
            FROM 'C:\Users\Tajammal khalid\Downloads\source_crm\prd_info.csv'
            WITH
            (
                FIRSTROW =2,
                FIELDTERMINATOR=',',
                TABLOCK
             );
            SET @end_time = GETDATE();
            PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';

            SET @start_time = GETDATE();
            PRINT '>Truncate Table crm_sales_details';
            PRINT '>Insert Data Into crm_sales_details';
            TRUNCATE TABLE bronze.crm_sales_details;
            BULK INSERT bronze.crm_sales_details
            FROM 'C:\Users\Tajammal khalid\Downloads\source_crm\sales_details.csv'
            WITH
            (
                FIRSTROW =2,
                FIELDTERMINATOR=',',
                TABLOCK
             );
            SET @end_time = GETDATE();
            PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';

            PRINT '--------------------------------------------';
            PRINT 'loading ERP Tables';
            PRINT '--------------------------------------------';
 

            SET @start_time = GETDATE();
            PRINT '>Truncate Table erp_loc_a101';
            PRINT '>Insert Data Into erp_loc_a101';
             TRUNCATE TABLE bronze.erp_loc_a101;
            BULK INSERT bronze.erp_loc_a101
            FROM 'C:\Users\Tajammal khalid\Downloads\source_erp\LOC_A101.csv'
            WITH
            (
                FIRSTROW =2,
                FIELDTERMINATOR=',',
                TABLOCK
             );
            SET @end_time = GETDATE();
            PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';
            SET @start_time = GETDATE();
            PRINT '>Truncate Table erp_cust_az12';
            PRINT '>Insert Data Into erp_cust_az12';

            TRUNCATE TABLE bronze.erp_cust_az12;
            BULK INSERT bronze.erp_cust_az12
            FROM 'C:\Users\Tajammal khalid\Downloads\source_erp\CUST_AZ12.csv'
            WITH
            (
                FIRSTROW =2,
                FIELDTERMINATOR=',',
                TABLOCK
             );

            SET @end_time = GETDATE();
            PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';
            SET @start_time = GETDATE();
            PRINT '>Truncate Table erp_cat_g1v2';
            PRINT '>Insert Data Into erp_cat_g1v2';


             TRUNCATE TABLE bronze.erp_cat_g1v2;
            BULK INSERT bronze.erp_cat_g1v2
            FROM 'C:\Users\Tajammal khalid\Downloads\source_erp\PX_CAT_G1V2.csv'
            WITH
            (
                FIRSTROW =2,
                FIELDTERMINATOR=',',
                TABLOCK
             );
            PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Second';
            SET @start_time = GETDATE();
            SET @bach_end_time = GETDATE();
            PRINT '===========================================';
            PRINT 'loading Bronze Complete';
            PRINT '- Total Load Duration: '+CAST(DATEDIFF(second,@bach_start_time,@bach_end_time)as  NVARCHAR) +'Second';
            PRINT '===========================================';
    END TRY
    BEGIN CATCH
    PRINT '===========================================';
    PRINT 'ERROR OCCURED DURING LODING BRONZE LAYER';
    PRINT 'Error Message' + ERROR_MESSAGE();
    PRINT 'Error Message' + CAST( ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message' + CAST( ERROR_STATE()AS NVARCHAR);
    PRINT '===========================================';

    END CATCH
END
