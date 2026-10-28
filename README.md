# azure-hrp-medallion-lakehouse-
End-to-End Enterprise HRP &amp; Payroll Data Lakehouse Pipeline using Azure Data Factory (ADF), ADLS Gen2, Azure Databricks (PySpark), Delta Lake (Medallion Architecture), and MS SQL Server.

# Enterprise HRP & Payroll Medallion Lakehouse Pipeline (Azure)

## 📌 Project Overview
This project demonstrates an end-to-end **Azure Data Engineering pipeline** that migrates and processes enterprise **Human Resource & Payroll (HRP)** transactional data (Employees, Payroll Runs, Leave & Time Tracking) from an on-premise **MS SQL Server** database into an **Azure Data Lakehouse (ADLS Gen2 + Delta Lake)** using the **Medallion Architecture (Bronze → Silver → Gold)**.

## 🏗️ Architecture & Tech Stack
* **Source Database:** MS SQL Server (T-SQL, Stored Procedures, Watermark CDC)
* **Ingestion & Orchestration:** Azure Data Factory (ADF) — Lookup, ForEach, Copy Activity, Stored Procedure Activity
* **Storage Layer:** Azure Data Lake Storage Gen2 (ADLS Gen2) & Delta Lake
* **Processing & Transformation:** Azure Databricks (PySpark, Spark SQL)
* **Data Modeling:** SCD Type 2 (Silver Layer) & Star Schema Fact/Dimension tables (Gold Layer)
* **BI & Reporting:** Power BI & SSRS Paginated Reports

## 🔄 Pipeline Workflow
1. **Incremental Ingestion (ADF → Bronze):** Metadata-driven ADF pipeline reads `ctl.WatermarkTable` in SQL Server, extracts only newly inserted/updated HRP records (`LastModifiedDate > WatermarkValue`), and writes raw Parquet files to the `bronze/` container in ADLS Gen2.
2. **Cleansing & SCD Type 2 Merge (Bronze → Silver):** PySpark notebook in Azure Databricks deduplicates records, validates schemas, masks sensitive PII, and performs a Delta Lake `MERGE INTO` to maintain **SCD Type 2 historical tracking** of employee department and salary band changes.
3. **Curated Star Schema & KPIs (Silver → Gold):** Spark SQL & PySpark Window functions aggregate monthly payroll payouts, overtime hours, tax deductions, and department attrition metrics into `Fact_Payroll_Monthly` and `Dim_Employee` tables for executive BI dashboards.
