📘 README – Silver Layer Load Procedure (silver.load_silver)

This document explains the purpose, logic, and transformations of the stored procedure silver.load_silver, which fully rebuilds the Silver Layer of the SQL Server Data Warehouse.

🚀 Overview

The Silver Layer processes data from the Bronze Layer by applying:

Data cleaning

Standardization

Validation

Enrichment

Auditing timestamps

Running the stored procedure:

Drops and recreates all Silver tables

Reloads all tables with transformed, cleaned data

Ensures complete consistency and repeatability

Adds audit timestamps for every loaded record

This provides a deterministic, rebuildable Silver layer.

▶️ How to Execute

To run the entire Silver Layer refresh:

EXEC silver.load_silver;

This command will rebuild all Silver staging tables in one batch process.

📦 Silver Tables Created & Loaded

The procedure produces and loads the following tables:

silver.crm_cust_info

silver.crm_prd_info

silver.crm_sales_details

silver.erp_cust_az12

silver.erp_loc_a101

silver.erp_px_cat_g1v2

Each table is dropped (if it exists) and then recreated with the expected schema to prevent drift or inconsistencies.

🧼 Key Transformations (By Table)
1️⃣ CRM Customer Info (crm_cust_info)

Data Deduplication

Uses ROW_NUMBER() to keep only the latest row per customer (based on cst_id).

Cleaning

Trims whitespace from names.

Removes customers missing cst_lastname.

Normalization

Marital Status:

M → Married

S → Single

All others → n/a

Gender:

M → Male

F → Female

All others → n/a

Audit

dwh_create_date is assigned with GETDATE().

2️⃣ CRM Product Info (crm_prd_info)

Category Derivation

cat_id created from the first 5 characters of prd_key (hyphens replaced with underscores).

prd_key suffix extracted from character 7 onward.

Data Cleaning

Null prd_cost values set to 0.

Product Line Normalization

R → Road

S → Other Sales

M → Mountain

T → Touring

others → n/a

Date Standardization

Converts prd_start_dt to DATE.

Computes prd_end_dt using the next start date minus one day.

Audit

dwh_create_date recorded with GETDATE().

3️⃣ CRM Sales Details (crm_sales_details)

Date Validation

Converts 8-digit integers (YYYYMMDD) to DATE.

Sets invalid or malformed dates to NULL.

Sales Fix Rules

If sales are invalid or don’t equal quantity × price:

Recalculate using quantity × ABS(price)

Price Fix Rules

If price invalid:

Calculate as sales divided by quantity

Audit

dwh_create_date assigned for every row.

4️⃣ ERP Customer AZ12 (erp_cust_az12)

Data Cleanup

Removes NAS prefix from customer IDs.

Replaces future birthdates with NULL.

Gender Standardization

Female: F or FEMALE

Male: M or MALE

All else → n/a

Audit

dwh_create_date added via GETDATE().

5️⃣ ERP Location A101 (erp_loc_a101)

Field Cleanup

Removes hyphens in customer IDs.

Country Normalization

DE → Germany

USA → United States

US → United States

empty → n/a

all other values remain unchanged

Audit

dwh_create_date added for each row.

6️⃣ ERP PX Category G1V2 (erp_px_cat_g1v2)

Transformations

Trims category field (cat).

Retains original subcat and maintenance.

Audit

Adds dwh_create_date with current timestamp.

⏱️ Audit & Timing

The procedure internally uses:

Batch-level timestamps

Step-level timestamps

GETDATE() for record-level auditing

This makes the load:

Fully traceable

Easy to monitor

Easy to extend with run-history logging

You can later add:

Row counts

Millisecond timings

Log tables

Just like the Bronze ingest procedure.

📌 Notes

✔ Silver layer is fully rebuildable because Bronze acts as the immutable source.
✔ Drop + create ensures schema correctness across every run.
✔ Deterministic transformations guarantee repeatability.
✔ Easy to extend with error logging or run history.
✔ Purpose-built for Data Warehouse ETL pipelines.