/*
====================================================
🏗️ Silver Layer Load Script – CRM Customer Info
====================================================
🧩 Description:
- Creates table if it doesn't exist
- Inserts transformed data from bronze.crm_cust_info
- Excludes rows where cst_lastname IS NULL
- Preserves dwh_created_date for audit trail
====================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @StartTime DATETIME2,
        @EndTime DATETIME2,
        @BatchStart DATETIME2 = SYSDATETIME(),
        @BatchEnd DATETIME2,
        @RowCount NVARCHAR(20),
        @log NVARCHAR(MAX) = N'';

    BEGIN TRY
        -------------------------------------------------------------------------
        -- HEADER
        -------------------------------------------------------------------------
        SET @log += N'===============================================================================' + CHAR(13)+CHAR(10);
        SET @log += N'🚀 Silver Layer Transformation for CRM and ERP Source Systems' + CHAR(13)+CHAR(10);
        SET @log += N'Start Time: ' + CONVERT(NVARCHAR(25), @BatchStart, 120) + CHAR(13)+CHAR(10);
        SET @log += N'===============================================================================' + CHAR(13)+CHAR(10);


        /*=======================================================================
          1. CRM CUSTOMER INFO
        =======================================================================*/
        SET @log += N'>>> Loading silver.crm_cust_info' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();

        IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
            DROP TABLE silver.crm_cust_info;

        EXEC('
            CREATE TABLE silver.crm_cust_info (
                cst_id INT,
                cst_key NVARCHAR(50),
                cst_firstname NVARCHAR(100),
                cst_lastname NVARCHAR(100),
                cst_marital_status NVARCHAR(20),
                cst_gndr NVARCHAR(20),
                cst_create_date DATETIME,
                dwh_create_date DATETIME2 DEFAULT GETDATE()
            );
        ');

        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date, dwh_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'M' THEN 'Married'
                WHEN 'S' THEN 'Single'
                ELSE 'n/a'
            END,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'n/a'
            END,
            cst_create_date,
            GETDATE()
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
        ) x
        WHERE rn = 1 AND cst_lastname IS NOT NULL;

        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = COUNT(*) FROM silver.crm_cust_info;

        SET @log += N'    ✔ Loaded silver.crm_cust_info → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);


        /*=======================================================================
          2. CRM PRODUCT INFO
        =======================================================================*/
        SET @log += N'>>> Loading silver.crm_prd_info' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();

        IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
            DROP TABLE silver.crm_prd_info;

        EXEC('
            CREATE TABLE silver.crm_prd_info (
                prd_id INT,
                cat_id NVARCHAR(50),
                prd_key NVARCHAR(50),
                prd_nm NVARCHAR(100),
                prd_cost INT,
                prd_line NVARCHAR(50),
                prd_start_dt DATE,
                prd_end_dt DATE,
                dwh_create_date DATETIME2 DEFAULT GETDATE()
            );
        ');

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt, dwh_create_date
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5),'-','_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'M' THEN 'Mountain'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE),
            GETDATE()
        FROM bronze.crm_prd_info;

        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = COUNT(*) FROM silver.crm_prd_info;

        SET @log += N'    ✔ Loaded silver.crm_prd_info → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);


        /*=======================================================================
          3. CRM SALES DETAILS
        =======================================================================*/
        SET @log += N'>>> Loading silver.crm_sales_details' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();

        IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
            DROP TABLE silver.crm_sales_details;

        EXEC('
            CREATE TABLE silver.crm_sales_details (
                sls_ord_num NVARCHAR(50),
                sls_prd_key NVARCHAR(50),
                sls_cust_id INT,
                sls_order_dt DATE,
                sls_ship_dt DATE,
                sls_due_dt DATE,
                sls_sales INT,
                sls_quantity INT,
                sls_price INT,
                dwh_create_date DATETIME2 DEFAULT GETDATE()
            );
        ');

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price, dwh_create_date
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) END,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0
                      OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price END,
            GETDATE()
        FROM bronze.crm_sales_details;

        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = COUNT(*) FROM silver.crm_sales_details;

        SET @log += N'    ✔ Loaded silver.crm_sales_details → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);


        /*=======================================================================
          4. ERP CUST AZ12
        =======================================================================*/
        SET @log += N'>>> Loading silver.erp_cust_az12' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();

        IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
            DROP TABLE silver.erp_cust_az12;

        EXEC('
            CREATE TABLE silver.erp_cust_az12 (
                cid NVARCHAR(50),
                bdate DATE,
                gen NVARCHAR(10),
                dwh_create_date DATETIME2 DEFAULT GETDATE()
            );
        ');

        INSERT INTO silver.erp_cust_az12 (
            cid, bdate, gen, dwh_create_date
        )
        SELECT
            CASE WHEN CID LIKE 'NAS%' THEN TRIM(SUBSTRING(CID, 4, LEN(CID)))
                 ELSE CID END,
            CASE WHEN BDATE > GETDATE() THEN NULL ELSE BDATE END,
            CASE
                WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END,
            GETDATE()
        FROM bronze.erp_cust_az12;

        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = COUNT(*) FROM silver.erp_cust_az12;

        SET @log += N'    ✔ Loaded silver.erp_cust_az12 → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);


        /*=======================================================================
          5. ERP LOC A101
        =======================================================================*/
        SET @log += N'>>> Loading silver.erp_loc_a101' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();

        IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
            DROP TABLE silver.erp_loc_a101;

        EXEC('
            CREATE TABLE silver.erp_loc_a101 (
                cid NVARCHAR(50),
                cntry NVARCHAR(50),
                dwh_create_date DATETIME2 DEFAULT GETDATE()
            );
        ');

        INSERT INTO silver.erp_loc_a101 (
            cid, cntry, dwh_create_date
        )
        SELECT
            REPLACE(cid,'-',''),
            CASE TRIM(cntry)
                WHEN 'DE' THEN 'Germany'
                WHEN 'USA' THEN 'United States'
                WHEN 'US' THEN 'United States'
                WHEN '' THEN 'n/a'
                ELSE cntry
            END,
            GETDATE()
        FROM bronze.erp_loc_a101;

        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = COUNT(*) FROM silver.erp_loc_a101;

        SET @log += N'    ✔ Loaded silver.erp_loc_a101 → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);


        /*=======================================================================
          6. ERP PX CAT G1V2
        =======================================================================*/
        SET @log += N'>>> Loading silver.erp_px_cat_g1v2' + CHAR(13)+CHAR(10);
        SET @StartTime = SYSDATETIME();

        IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
            DROP TABLE silver.erp_px_cat_g1v2;

        EXEC('
            CREATE TABLE silver.erp_px_cat_g1v2 (
                id NVARCHAR(50),
                cat NVARCHAR(50),
                subcat NVARCHAR(50),
                maintenance NVARCHAR(50),
                dwh_create_date DATETIME2 DEFAULT GETDATE()
            );
        ');

        INSERT INTO silver.erp_px_cat_g1v2 (
            id, cat, subcat, maintenance, dwh_create_date
        )
        SELECT
            id,
            TRIM(cat),
            subcat,
            maintenance,
            GETDATE()
        FROM bronze.erp_px_cat_g1v2;

        SET @EndTime = SYSDATETIME();
        SELECT @RowCount = COUNT(*) FROM silver.erp_px_cat_g1v2;

        SET @log += N'    ✔ Loaded silver.erp_px_cat_g1v2 → ' + @RowCount + N' rows in '
                  + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' s ('
                  + CAST(DATEDIFF(MILLISECOND,@StartTime,@EndTime) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);


        /*=======================================================================
          SUMMARY
        =======================================================================*/
        SET @BatchEnd = SYSDATETIME();

        SET @log += N'-------------------------------------------------------------------------------' + CHAR(13)+CHAR(10);
        SET @log += N'✅ Silver Layer Transformation Completed Successfully' + CHAR(13)+CHAR(10);
        SET @log += N'Total Duration: '
                     + CAST(DATEDIFF(SECOND,@BatchStart,@BatchEnd) AS NVARCHAR(10)) + N' s ('
                     + CAST(DATEDIFF(MILLISECOND,@BatchStart,@BatchEnd) AS NVARCHAR(10)) + N' ms)' + CHAR(13)+CHAR(10);
        SET @log += N'End Time: ' + CONVERT(NVARCHAR(25), @BatchEnd, 120) + CHAR(13)+CHAR(10);
        SET @log += N'===============================================================================' + CHAR(13)+CHAR(10);

        PRINT @log;

        INSERT INTO bronze.ingest_data_run_history (log, duration_seconds, status)
        VALUES (@log, DATEDIFF(SECOND, @BatchStart, @BatchEnd), N'SUCCESS');

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX);
        SET @ErrorMessage = N'❌ Error during Silver transformation: ' + ERROR_MESSAGE();

        PRINT @ErrorMessage;

        INSERT INTO bronze.ingest_data_run_history (log, status)
        VALUES (@ErrorMessage, N'FAILURE');
    END CATCH
END;


EXEC silver.load_silver


/*Validation*/
SELECT * FROM silver.crm_cust_info
SELECT * FROM silver.crm_prd_info
SELECT * FROM silver.crm_sales_details
SELECT * FROM silver.erp_cust_az12
SELECT * FROM silver.erp_loc_a101
SELECT * FROM silver.erp_px_cat_g1v2