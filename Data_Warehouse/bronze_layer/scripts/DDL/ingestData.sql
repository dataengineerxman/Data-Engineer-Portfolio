/*
====================================================
🏗️ Data Warehouse Ingest Data Script (Final Version)
====================================================
🧩 Description:
- Loads CRM + ERP CSVs into Bronze tables
- Tracks load time per table (s + ms)
- Shows row counts per table
- Prints one clean combined log block
- Stores execution summary in bronze.run_history
====================================================
*/
CREATE OR ALTER PROCEDURE bronze.ingest_data AS
BEGIN
    DECLARE
        @StartTime DATETIME,
        @EndTime DATETIME,
        @BatchStart DATETIME = GETDATE(),
        @BatchEnd DATETIME,
        @log NVARCHAR(MAX) = N'',
        @RowCount NVARCHAR(20);

    BEGIN TRY
        SET @log += N'===============================================================================' + CHAR(13)+CHAR(10);
        SET @log += N'🚀 Bronze Layer Data Ingestion for CRM and ERP Source Systems' + CHAR(13)+CHAR(10);
        SET @log += N'Start Time: ' + CONVERT(NVARCHAR(20), @BatchStart, 120) + CHAR(13)+CHAR(10);
        SET @log += N'===============================================================================' + CHAR(13)+CHAR(10);

        ------------------------------------------------------------
        -- CRM Customer Info
        ------------------------------------------------------------
        SET @log += N'>>> Truncating table bronze.crm_cust_info' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Data_Engineering_Material\SQL\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = CAST(COUNT(*) AS NVARCHAR(20)) FROM bronze.crm_cust_info;

        SET @log += N'    ✔ Loaded bronze.crm_cust_info → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);

        ------------------------------------------------------------
        -- CRM Product Info
        ------------------------------------------------------------
        SET @log += N'>>> Truncating table bronze.crm_prd_info' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Data_Engineering_Material\SQL\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = CAST(COUNT(*) AS NVARCHAR(20)) FROM bronze.crm_prd_info;

        SET @log += N'    ✔ Loaded bronze.crm_prd_info → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);

        ------------------------------------------------------------
        -- CRM Sales Details
        ------------------------------------------------------------
        SET @log += N'>>> Truncating table bronze.crm_sales_details' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Data_Engineering_Material\SQL\DataWarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = CAST(COUNT(*) AS NVARCHAR(20)) FROM bronze.crm_sales_details;

        SET @log += N'    ✔ Loaded bronze.crm_sales_details → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);

        ------------------------------------------------------------
        -- ERP Customer AZ12
        ------------------------------------------------------------
        SET @log += N'>>> Truncating table bronze.erp_cust_az12' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Data_Engineering_Material\SQL\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = CAST(COUNT(*) AS NVARCHAR(20)) FROM bronze.erp_cust_az12;

        SET @log += N'    ✔ Loaded bronze.erp_cust_az12 → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);

        ------------------------------------------------------------
        -- ERP Location A101
        ------------------------------------------------------------
        SET @log += N'>>> Truncating table bronze.erp_loc_a101' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Data_Engineering_Material\SQL\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = CAST(COUNT(*) AS NVARCHAR(20)) FROM bronze.erp_loc_a101;

        SET @log += N'    ✔ Loaded bronze.erp_loc_a101 → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);

        ------------------------------------------------------------
        -- ERP PX_CAT_G1V2
        ------------------------------------------------------------
        SET @log += N'>>> Truncating table bronze.erp_px_cat_g1v2' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Data_Engineering_Material\SQL\DataWarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = CAST(COUNT(*) AS NVARCHAR(20)) FROM bronze.erp_px_cat_g1v2;

        SET @log += N'    ✔ Loaded bronze.erp_px_cat_g1v2 → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);

        ------------------------------------------------------------
        -- Summary
        ------------------------------------------------------------
        SET @BatchEnd = SYSDATETIME();
        SET @log += N'===============================================================================' + CHAR(13)+CHAR(10);
        SET @log += N'✅ Data Ingestion Completed Successfully' + CHAR(13)+CHAR(10);
        SET @log += N'Total Duration: ' + CAST(DATEDIFF(SECOND,@BatchStart,@BatchEnd) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@BatchStart,@BatchEnd) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);
        SET @log += N'End Time: ' + CONVERT(NVARCHAR(20), @BatchEnd, 120) + CHAR(13)+CHAR(10);
        SET @log += N'===============================================================================' + CHAR(13)+CHAR(10);

        PRINT @log;

        -- =====================================================
        -- 🧾 Store run summary into bronze.run_history
        -- =====================================================
        INSERT INTO bronze.ingest_data_run_history (log, duration_seconds, status)
        VALUES (@log, DATEDIFF(SECOND, @BatchStart, @BatchEnd), N'SUCCESS');

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMsg NVARCHAR(MAX);
        SET @ErrorMsg = N'❌ Error during ingestion: ' + ERROR_MESSAGE();

        PRINT @ErrorMsg;

        -- Log failure in run_history
        INSERT INTO bronze.ingest_data_run_history (log, status)
        VALUES (@ErrorMsg, N'FAILURE');
    END CATCH
END;
GO

