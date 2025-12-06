🧱 1. Dimension: gold.dim_customer
🎯 Purpose

Provides descriptive information about customers for segmentation, filtering, grouping, and reporting.

🏗 Source Tables

From the Silver Layer:

silver.crm_cust_info

silver.erp_cust_az12

silver.erp_loc_a101

🧬 Transformations

A surrogate key is generated using ROW_NUMBER() → customer_key

Gender is derived using prioritized logic:

If CRM gender ≠ 'n/a', use it

Else fallback to ERP gender

Country and birthdate sourced from ERP tables

Customer creation date preserved from CRM

📄 Final Output Columns
Column	Description
customer_key	Surrogate primary key
customer_id	Natural customer ID
customer_number	CRM customer key
first_name	Customer first name
last_name	Customer last name
country	Country from ERP
birthdate	Birthdate from ERP
marital_status	CRM marital status
gender	Merged customer gender
create_date	CRM account creation date
📦 2. Dimension: gold.dim_products
🎯 Purpose

Stores attributes describing products, categories, and product lines for product-level analytics.

🏗 Source Tables

From the Silver Layer:

silver.crm_prd_info

silver.erp_px_cat_g1v2

🧬 Transformations

Surrogate key created with ROW_NUMBER() → product_key

Category enrichments added via ERP category table

Only active products are included (prd_end_dt IS NULL)

📄 Final Output Columns
Column	Description
product_key	Surrogate primary key
product_id	Natural product ID
product_number	CRM product key
product_name	Product name
category_id	Category foreign key
sub_category	Category summary
maintenance	Maintenance flag
cost	Product cost
product_line	Category grouping
start_date	Product introduction date
💰 3. Fact Table: gold.fact_sales
🎯 Purpose

Stores sales transactions, linking customers and products to order metrics (sales, quantity, price).

🏗 Source Table

From the Silver Layer:

silver.crm_sales_details

🔗 Relationships

Fact table joins:

product_key → gold.dim_products.product_key

customer_key → gold.dim_customer.customer_key

📄 Final Output Columns
Column	Description
order_number	Primary key for each sales order line
product_key	FK → product dimension
customer_key	FK → customer dimension
order_date	Order creation date
shipping_date	Order shipment date
due_date	Due delivery date
sales_amount	Total dollar amount
quantity	Units sold
price	Unit price
🔄 Data Lineage Summary
 Silver Layer                Gold Layer
--------------------------------------------------
 crm_cust_info   ───────►   dim_customer
 erp_cust_az12   ───────►   dim_customer
 erp_loc_a101    ───────►   dim_customer

 crm_prd_info    ───────►   dim_products
 erp_px_cat_g1v2 ───────►   dim_products

 crm_sales_details ─────►   fact_sales
                         JOIN dim_products
                         JOIN dim_customer

🧠 Business Value

✔ Single source of truth for sales, customers, and products
✔ Surrogate keys ensure warehouse consistency
✔ Optimized star schema for BI tools: Power BI, Tableau, Looker
✔ Consistent, enriched attributes for reporting & ML models
✔ Filters & joins simplified across the organization

If you'd like, I can also create:

📌 A visual Star Schema diagram
📌 A Markdown architecture diagram
📌 A Snowflake SQL script to create physical tables
📌 README badges, emojis, and collapsible sections