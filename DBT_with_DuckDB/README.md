NYC Parking Violations Data Pipeline
This project implements a robust ELT (Extract, Load, Transform) pipeline using dbt (data build tool) and DuckDB to analyze New York City parking violation data. It transforms raw CSV data into a structured dimensional model following a Medallion Architecture (Bronze, Silver, Gold).

🚀 Overview
The goal of this project is to process large-scale public data to identify trends in parking violations, calculate potential revenue, and analyze vehicle demographics in NYC.

Database: DuckDB (In-process OLAP)

Transformation: dbt (Core 1.6+)

Data Source: NYC Open Data - Parking Violations Issued

🏗️ Architecture
The project follows a modular design to ensure data quality and lineage:

1. Bronze Layer (Raw)
Materialization: ephemeral

Focus: Direct ingestion of raw CSV data with minimal changes. Fields are cast to appropriate types but retain original granularity.

2. Silver Layer (Cleaned)
Materialization: view

Focus: * Standardizing column names to snake_case.

Joining violation codes with their respective descriptions and fines.

Calculating flags for specific geographic zones (e.g., Manhattan below 96th St).

3. Gold Layer (Curated)
Materialization: table

Focus: Business-ready metrics and aggregations.

gold_ticket_metrics: Revenue and volume by violation type.

gold_vehicles_metrics: Analysis of repeat offenders by registration state and vehicle make.


Run the pipeline:
Bash
dbt clean
dbt deps
dbt build

🧪 Data Quality & Testing
Data integrity is enforced through dbt tests:

Generic Tests: unique and not_null constraints on summons_number.

Custom Tests: Implemented custom_not_null macros to ensure critical business fields are never empty.

📊 Key Insights
Revenue Mapping: Integrated violation codes with fine amounts to estimate total city revenue from different violation types.

Geographic Analysis: Distinguished between higher-fine zones (Lower Manhattan) and other areas.

Project Structure
Plaintext
├── macros/              # Custom Jinja logic (schema names, custom tests)
├── models/
│   ├── bronze/          # Raw data staging
│   ├── silver/          # Cleaned, joined views
│   └── gold/            # Final aggregate tables
├── dbt_project.yml      # Project configuration
└── profiles.yml         # Connection settings