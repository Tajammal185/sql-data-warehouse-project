/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze → Silver)
===============================================================================
Purpose:
    This stored procedure executes the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables using data from the 'bronze' layer.

Actions Performed:
    - Truncates the existing Silver tables.
    - Inserts transformed and cleansed data from Bronze into the Silver tables.

Parameters:
    None.
    This procedure does not accept input parameters or return values.

Usage:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME ,@bach_start_time DATETIME , @bach_end_time DATETIME;
	BEGIN TRY
	        SET @bach_start_time = GETDATE();
            PRINT '===========================================';
            PRINT 'loading sILVER layer';
            PRINT '===========================================';


            PRINT '--------------------------------------------';
            PRINT 'loading CRM Tables';
            PRINT '--------------------------------------------';

            SET @start_time = GETDATE();
            PRINT '>Truncate Table silver.crm_cust_info';
            PRINT '>Insert Data Into silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;    
		INSERT INTO   silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cts_lastname,
		cts_material_status,
		cst_gndr,
		cst_create_date
		)
		SELECT 
		cst_id ,
		cst_key ,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cts_lastname ) as cts_lastname,
		CASE 
			WHEN UPPER(TRIM(cts_material_status))='S' THEN 'Single'
			WHEN UPPER(TRIM(cts_material_status))='M' THEN 'Marriage'
			ELSE 'n/a'
		END cts_material_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date 

		FROM
		(
			SELECT *,
			ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)as t 
		WHERE flag_last=1
		SET @end_time = GETDATE();
        PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';




        SET @start_time = GETDATE();
        PRINT '>Truncate Table silver.crm_prd_info';
        PRINT '>Insert Data Into silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prt_cost,
		prd_line,
		prdt_start_dt,
		prd_end_dt)
		select 
		prd_id ,
		REPLACE(SUBSTRING(prd_key ,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prt_cost ,0) AS prt_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line ,
		CAST(prdt_start_dt AS DATE ) AS prdt_start_dt,
		CAST(LEAD (prd_end_dt) OVER (PARTITION BY  prd_key ORDER BY prdt_start_dt ) -1 AS DATE ) AS prd_end_dt
		from bronze.crm_prd_info 
		SET @end_time = GETDATE();
        PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';

        SET @start_time = GETDATE();
        PRINT '>Truncate Table crm_sales_details';
        PRINT '>Insert Data Into crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO  silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key ,
		sls_cust_id  ,
		sls_order_dt ,
		sls_ship_dt  ,
		sla_due_dt,
		sls_sales,
		sla_quantity ,
		sla_price 

		)
		select 
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		CASE 
			WHEN sls_order_dt=0 OR LEN(sls_order_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS NVARCHAR )AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt=0 OR LEN(sls_ship_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS NVARCHAR )AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sla_due_dt=0 OR LEN(sla_due_dt) !=8 THEN NULL
			ELSE CAST(CAST(sla_due_dt AS NVARCHAR )AS DATE)
		END AS sla_due_dt,


		CASE 
			WHEN sls_sales IS  NULL OR sls_sales <= 0 OR sls_sales != sla_quantity *ABS(sla_price)
				THEN sla_quantity * ABS(sla_price)
			ELSE sls_sales
		END AS sls_sales,

	
		sla_quantity ,
		CASE
			WHEN sla_price IS NULL OR sla_price <=0
				THEN sls_sales / NULLIF(sla_quantity,0)
			ELSE sls_sales
		END AS sls_sales

		from bronze.crm_sales_details
        SET @end_time = GETDATE();
        PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';

         PRINT '--------------------------------------------';
         PRINT 'loading ERP Tables';
         PRINT '--------------------------------------------';
 

        SET @start_time = GETDATE();
        PRINT '>Truncate Table erp_loc_a101';
        PRINT '>Insert Data Into erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)

		SELECT 
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE 
			WHEN bdate >GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(gen)) IN('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen 
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
        PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';
        
		
		SET @start_time = GETDATE();
        PRINT '>Truncate Table erp_cust_az12';
        PRINT '>Insert Data Into erp_cust_az12';

        TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_loc_a101
		(
		cid,
		cntry
		)

		select 
		REPLACE(cid,'-','')cid,
		CASE 
			WHEN TRIM(cntry)='DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA')THEN 'United States'
			WHEN TRIM (cntry) ='' OR cntry IS Null THEN 'n/a'
			ELSE cntry
		END cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
        PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Scound';
        
		
		SET @start_time = GETDATE();
        PRINT '>Truncate Table erp_cat_g1v2';
        PRINT '>Insert Data Into erp_cat_g1v2';
        TRUNCATE TABLE silver.erp_cat_g1v2;
		INSERT INTO silver.erp_cat_g1v2
		(
		id,cat,subcat,maintenance
		)

		select id,cat,subcat,maintenance from bronze.erp_cat_g1v2
	        PRINT 'Load Duration:'+CAST(DATEDIFF(second,@start_time,@end_time)as nvarchar)+' Second';
            SET @start_time = GETDATE();
            SET @bach_end_time = GETDATE();
            PRINT '===========================================';
            PRINT 'loading Silver Complete';
            PRINT '- Total Load Duration: '+CAST(DATEDIFF(second,@bach_start_time,@bach_end_time)as  NVARCHAR) +'Second';
            PRINT '===========================================';
	
	END TRY
    BEGIN CATCH
    PRINT '===========================================';
    PRINT 'ERROR OCCURED DURING LODING silver LAYER';
    PRINT 'Error Message' + ERROR_MESSAGE();
    PRINT 'Error Message' + CAST( ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message' + CAST( ERROR_STATE()AS NVARCHAR);
    PRINT '===========================================';

    END CATCH
END
