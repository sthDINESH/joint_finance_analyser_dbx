# SpendSense
![SpendSense banner](images/banner.webp)

## Project Overview

> Ever looked at your bank statements and thought, "I recognise all these transactions, so why does none of this make sense?"

That was the idea behind **`SpendSense`**: a Databricks-based financial analytics project built to clarify where the money is going.

It brings together **joint-account statements** and **monthly budgets**, turning a jumble of transactions and good intentions into a clearer view of everyday finances.

The dashboard is where planned spending meets reality. It compares budgets with actual outgoings, tracks income and balances, and highlights month-to-month changes, helping turn the question "Where did the money go?" into something easier to answer.

## Key Features

• `Data Ingestion from Google Drive` 

• `Statements and Budget ETL Pipelines` • `Data Cleaning and Standardisation` 

• `Rule-Based Categorisation` 

• `Monthly Aggregations` • `Month-to-Month Metrics` 

• `Curated Gold Tables` 

• `Databricks Dashboards`

## Architecture

**`SpendSense`** follows a Databricks `medallion architecture`. Google Drive is the source-data boundary, with statements and budgets moving through separate Bronze, Silver, and Gold layers. Each dataset has its own ETL pipeline and curated analytical tables before the dashboard brings the two Gold outputs together for reporting.

```mermaid
flowchart TD
    A[Google Drive] --> B[Statement Bronze table]
    A --> C[Budget Bronze table]
    B --> D[Statements ETL pipeline]
    C --> E[Budget ETL pipeline]
    D --> F[Statement Silver table]
    E --> G[Budget Silver table]
    F --> H[Statement Gold table]
    G --> I[Budget Gold table]
    H --> J[Databricks dashboard]
    I --> J
```

- [Databricks bundle configuration](bronze_to_silver_to_gold/databricks.yml) defines development and production deployment targets. 
- [Pipeline resources](bronze_to_silver_to_gold/resources) orchestrate the statement and budget workflows.
- [Dashboard definition](Joint%20account%20dashboard.lvdash.json) joins their Gold outputs for reporting.

## 4. Data Pipelines

### Statements ETL Pipeline

Implemented in the [statements transformations](bronze_to_silver_to_gold/src/Bronze-to-Silver-to-Gold-Statements-ETL-Pipeline/transformations) and configured by the [statements pipeline resource](bronze_to_silver_to_gold/resources/statements_etl.pipeline.yml), this pipeline:

1. Reads joint-account statement data from the Bronze table.
2. Removes account metadata and standardises column names and data types.
3. Categorises transactions using description keywords and debit or credit values.
4. Aggregates monthly incomings and outgoings by category.
5. Calculates changes from the previous month.

### Budget ETL Pipeline

Implemented in the [budget transformations](bronze_to_silver_to_gold/src/Bronze-to-Silver-to-Gold-Budget-ETL-Pipeline/transformations) and configured by the [budget pipeline resource](bronze_to_silver_to_gold/resources/budget_etl.pipeline.yml), this pipeline:

1. Reads monthly budget data from the Bronze table.
2. Converts the budget month into a date and reshapes budget columns into rows.
3. Categorises budget lines using the shared [category configuration](bronze_to_silver_to_gold/src/config/categories.py).
4. Aggregates monthly budget values by category.
5. Calculates changes from the previous month.

## 5. Technologies

[![Databricks](https://img.shields.io/badge/Databricks-EA1E2D?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/) [![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/) [![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/) [![SQL](https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://en.wikipedia.org/wiki/SQL) [![Google Drive](https://img.shields.io/badge/Google%20Drive-4285F4?style=for-the-badge&logo=googledrive&logoColor=white)](https://www.google.com/drive/)

- **Databricks** for data processing, pipelines, and analytics.
- **PySpark** for transformations and aggregations.
- **Databricks SQL** for dashboard datasets and analytical joins.
- **Databricks Declarative Automation Bundles** for deployment configuration.
- **Google Drive** as the source for statement and budget files.
- **Databricks dashboards** for exploring the resulting financial data.