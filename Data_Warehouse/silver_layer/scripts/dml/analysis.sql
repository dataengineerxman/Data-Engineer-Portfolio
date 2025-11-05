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