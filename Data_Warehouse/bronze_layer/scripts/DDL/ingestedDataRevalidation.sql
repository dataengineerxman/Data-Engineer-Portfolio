EXEC bronze.ingest_data;

/*CRM Customer Information Table Revalidation*/
SELECT COUNT(*) AS TotalRecords FROM bronze.crm_cust_info;

SELECT * FROM bronze.crm_cust_info;

/*CRM Product Information Table Revalidation*/
SELECT COUNT(*) AS TotalRecords FROM bronze.crm_prd_info;

SELECT * FROM bronze.crm_prd_info;

/*CRM Sales Details Table Revalidation*/
SELECT COUNT(*) AS TotalRecords FROM bronze.crm_sales_details;

SELECT * FROM bronze.crm_sales_details;

/*ERP Customer AZ12 Table Revalidation*/
SELECT COUNT(*) AS TotalRecords FROM bronze.erp_cust_az12;

SELECT * FROM bronze.erp_cust_az12;

/*ERP LOC a101 Table Revalidation*/
SELECT COUNT(*) AS TotalRecords FROM bronze.erp_loc_a101;

SELECT * FROM bronze.erp_loc_a101;

/*ERP PX CAT g1v2 Table Revalidation*/
SELECT COUNT(*) AS TotalRecords FROM bronze.erp_px_cat_g1v2;

SELECT * FROM bronze.erp_px_cat_g1v2;

/*Ingested Data */
SELECT COUNT(*) AS TotalRecords FROM bronze.ingest_data_run_history;

SELECT * FROM bronze.ingest_data_run_history;


