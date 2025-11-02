# 🏗️ Building the Data Warehouse (Data Engineering)

Welcome to the **Data Warehouse Project** — a modern data engineering initiative focused on designing and implementing a centralized, analytics-ready data warehouse using **SQL Server**.  

---

## 🎯 Project Objective

To build a **modern, scalable, and clean Data Warehouse** that consolidates sales data from multiple operational sources to:
- 📊 **Enable analytical reporting**
- 💡 **Inform decision-making**
- 🔁 **Ensure data consistency and quality**
- ⚙️ **Simplify integration between ERP and CRM systems**

---

## 🧩 Data Pipeline Overview

| Step | Description | Tools / Technology |
|------|--------------|--------------------|
| **1️⃣ Data Extraction** | Load flat files (CSV exports) from ERP and CRM systems | Python, SQL Server BULK INSERT |
| **2️⃣ Data Cleansing** | Handle missing values, inconsistent formats, and duplicates | SQL, Data Quality Rules |
| **3️⃣ Data Integration** | Combine both sources into a unified, user-friendly analytical model | SQL Server |
| **4️⃣ Data Modeling** | Implement a **star schema** optimized for analytics | SQL Server (T-SQL) |
| **5️⃣ Reporting Layer** | Enable BI/Analytics tools to consume the clean data | Power BI / Excel (optional) |

---

## 🧮 Data Sources

📁 **ERP Sales Data**  
Contains product-level transactions, order details, and financial metrics.  

📁 **CRM Customer Data**  
Includes customer profiles, regions, and segmentation attributes.  

> Both datasets are provided as CSV files and represent the **latest available snapshot** (no historization).

---

## 🧹 Data Quality & Integration Rules

- ✅ Remove duplicate records  
- 🧾 Standardize date and numeric formats  
- 🌎 Normalize country and region codes  
- 🔗 Match customers across ERP and CRM sources using unique identifiers  
- 📈 Validate numeric fields (e.g., SalesAmount ≥ 0)

---

## 🏗️ Data Model Overview

The final **Data Warehouse** follows a **Star Schema** pattern for simplicity and performance:

