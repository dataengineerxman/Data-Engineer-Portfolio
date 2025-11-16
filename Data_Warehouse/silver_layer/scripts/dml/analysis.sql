/*crm_cust_info join with crm_sales_details
cst_id=sls_cust_id

crm_prd_info join with crm_sales_details
prd_id=sls_prd_key

crm_cust_info join with erp.cid (substring from 3 characters)
cst_key=cid

crm_prd_info join with bronze.erp_px_cat_g1v2;substring
prd_key=id

crm_cust_info with bronze.erp_loc_a101;
cst_key=cid subtring*/

/*Check for Nulls and Duplicates in Primary Key Columns
  Expected: No results
  */
/*Cust Info Table*/
SELECT COUNT(*) FROM bronze.crm_cust_info;

SELECT cst_id,
       COUNT(*) AS DuplicateCount
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


SELECT COUNT(*) FROM silver.crm_cust_info;
SELECT * FROM silver.crm_cust_info;

SELECT cst_id,
       COUNT(*) AS DuplicateCount
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

/*Product Info Table*/
--Duplicates and Nulls in Primary Key Column
SELECT COUNT(*) FROM bronze.crm_prd_info;

SELECT prd_id,
         COUNT(*) AS DuplicateCount
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT COUNT(*) FROM silver.crm_prd_info;
