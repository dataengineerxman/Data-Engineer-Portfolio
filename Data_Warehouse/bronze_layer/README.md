🧱 1️⃣ 02_Create_Bronze_Tables.sql
🎯 Purpose

Creates all Bronze-level staging tables for both CRM and ERP source systems.

🧩 Details

Drops and recreates each table under the bronze schema.

Defines structured tables to receive data directly from CSVs.

Ensures consistent naming across CRM and ERP sources.

📊 Tables Created
Source	Table	Description
CRM	bronze.crm_cust_info	Customer master data
CRM	bronze.crm_prd_info	Product details
CRM	bronze.crm_sales_details	Sales transactions
ERP	bronze.erp_cust_az12	ERP customer master
ERP	bronze.erp_loc_a101	Location information
ERP	bronze.erp_px_cat_g1v2	Product category mapping
Meta	bronze.ingest_data_run_history	Stores ETL execution logs
⚙️ 2️⃣ bronze.ingest_data (Stored Procedure)
🎯 Purpose

Automates the data ingestion process for all CRM and ERP tables and records performance logs for each run.

🧩 Main Functions

Truncates existing data before each reload

BULK INSERTS new data from CSV files

Captures:

Rows loaded per table

Time taken per table (seconds + milliseconds)

Total duration of the run

Stores the complete run log in bronze.ingest_data_run_history

🧾 Example Output (as seen in SQL Server / PyCharm)
===============================================================================
🚀 Bronze Layer Data Ingestion for CRM and ERP Source Systems
Start Time: 2025-11-02 08:13:46
===============================================================================
>>> Truncating table bronze.crm_cust_info
✔ Loaded bronze.crm_cust_info → 18493 rows in 0 s (40 ms)
>>> Truncating table bronze.crm_prd_info
✔ Loaded bronze.crm_prd_info → 397 rows in 0 s (3 ms)
>>> Truncating table bronze.crm_sales_details
✔ Loaded bronze.crm_sales_details → 60398 rows in 0 s (117 ms)
>>> Truncating table bronze.erp_cust_az12
✔ Loaded bronze.erp_cust_az12 → 18483 rows in 0 s (30 ms)
>>> Truncating table bronze.erp_loc_a101
✔ Loaded bronze.erp_loc_a101 → 18484 rows in 0 s (20 ms)
>>> Truncating table bronze.erp_px_cat_g1v2
✔ Loaded bronze.erp_px_cat_g1v2 → 37 rows in 0 s (3 ms)
===============================================================================
✅ Data Ingestion Completed Successfully
Total Duration: 0 s (233 ms)
End Time: 2025-11-02 08:13:46
===============================================================================

🧾 Run Log Table: bronze.ingest_data_run_history

Each stored procedure execution automatically inserts a record here:

Column	Description
id	Auto-increment run ID
run_ts	Run timestamp
duration_seconds	Total runtime
status	✅ Success / ❌ Failed
log	Full execution details and timings

Example query:

SELECT TOP 5 run_ts, status, duration_seconds
FROM bronze.ingest_data_run_history
ORDER BY run_ts DESC;

🐍 3️⃣ run_ingest_log.py (Python Script)
🎯 Purpose

Provides an external launcher for the stored procedure —
allowing ingestion to be run from PyCharm, Command Line, or a Task Scheduler.

🧩 Key Actions

Connects to SQL Server via pyodbc

Executes the stored procedure:

EXEC bronze.ingest_data;


Retrieves and prints the most recent run log from bronze.ingest_data_run_history

Saves a timestamped copy of the log to /logs for auditing

📄 Example Output
🚀 Executing bronze.ingest_data ...
✅ Ingestion completed successfully.
📊 Latest run summary:
Run ID: 12 | Duration: 2.4 seconds | Status: Success
📝 Log saved to: D:\Data_Warehouse\logs\20251102_0813_ingest.log

📁 Folder Structure
Data_Warehouse/
│
├── sql/
│   ├── 01_Create_Schemas.sql
│   ├── 02_Create_Bronze_Tables.sql
│   ├── 03_ingest_data_procedure.sql
│
├── python/
│   └── run_ingest_log.py
│
├── datasets/
│   ├── source_crm/
│   └── source_erp/
│
├── logs/
│   └── YYYYMMDD_HHMM_ingest.log
│
└── README.md

🧠 Key Takeaways

✅ Clean separation between schema creation, data ingestion, and automation
✅ Performance metrics logged automatically per run
✅ Full run history table ensures auditability
✅ Python integration allows scheduled ETL orchestration

🧭 Silver Layer
Clone Bronze Tables into Silver Layer to start Data Transformation and Cleansing.
Add a dwh_date Metadata field to each Silver Table for SCD Type 2 implementation.

🧱 Data Analysis
Check for Nulls and Duplicates in Primary Key in bronze layer tables

