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

/* CRM CUSTOMER INFO*/
-- Step 1: Create table only if it does NOT exist
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.crm_cust_info;
    PRINT '🗑️ Dropped table silver.crm_cust_info';
END
ELSE
BEGIN
    PRINT 'ℹ️ Table silver.crm_cust_info did not exist — nothing to drop.';
END;
GO  -- IMPORTANT: separate batches so CREATE TABLE works properly

PRINT '📦 Creating table silver.crm_cust_info...';

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(100),
    cst_lastname       NVARCHAR(100),
    cst_marital_status NVARCHAR(20),
    cst_gndr           NVARCHAR(20),
    cst_create_date    DATETIME,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

PRINT '✅ Table silver.crm_cust_info created successfully.';
GO


-- Step 2: Insert transformed data
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    dwh_create_date
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
    END AS cst_marital_status,
    CASE UPPER(TRIM(cst_gndr))
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date,
    GETDATE() AS dwh_create_date  -- Audit timestamp
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) AS subquery
WHERE flag_last = 1
  AND cst_lastname IS NOT NULL;

PRINT '✅ Data successfully loaded into silver.crm_cust_info.';
GO

--Revalidate Results
SELECT * FROM silver.crm_cust_info


/* CRM PRODUCT INFO */
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.crm_prd_info;
    PRINT '🗑️ Dropped table silver.crm_prd_info';
END
ELSE
BEGIN
    PRINT 'ℹ️ Table silver.crm_prd_info did not exist.';
END
GO

PRINT '📦 Creating table silver.crm_prd_info...';
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
PRINT '✅ Table silver.crm_prd_info created successfully.';
GO

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    dwh_create_date
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Mountain'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt,
    GETDATE() AS dwh_create_date  -- Audit timestamp
FROM bronze.crm_prd_info;

/* CRM SALES DETAILS */


