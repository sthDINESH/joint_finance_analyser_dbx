# SpendSense
![SpendSense banner](images/banner.webp)

## 1. Project Overview

> Ever looked at your bank statements and thought, "I recognise all these transactions, so why does none of this make sense?"

That was the idea behind **`SpendSense`**: a Databricks-based financial analytics project built to clarify where the money is going.

It brings together **joint-account statements** and **monthly budgets**, turning a jumble of transactions and good intentions into a clearer view of everyday finances.

The dashboard is where planned spending meets reality. It compares budgets with actual outgoings, tracks income and balances, and highlights month-to-month changes, helping turn the question "Where did the money go?" into something easier to answer.

## 2. Key Features

- Ingests joint-account statements and monthly budget data from Google Drive.
- Cleans and standardises dates, amounts, descriptions, balances, and budget values.
- Categorises transactions and budget lines using configurable keywords.
- Joins planned budgets with actual statement activity in the dashboard layer.
- Aggregates financial activity by month and category.
- Calculates month-to-month changes for spending, income, and budgets.
- Calculates dashboard metrics such as net balance, cumulative balance, and budget buffer.
- Publishes curated Gold tables for dashboard analysis.

## 3. Architecture

SpendSense uses a Databricks medallion architecture. The repository documents the transformation and deployment layers; the ingestion process that creates the Bronze tables is treated as the source-data boundary.

```text
Google Drive statements and budgets
					 |
					 v
		 Bronze ingestion tables
					 |
					 v
		 Silver cleaning and categorisation
					 |
					 v
		 Gold monthly analytical tables
					 |
					 v
				 Dashboards
```

Statements and budgets follow separate ETL pipelines, but both use the same Bronze, Silver, and Gold pattern. The [Databricks bundle configuration](bronze_to_silver_to_gold/databricks.yml) defines development and production targets, while the [dashboard definition](Joint%20account%20dashboard.lvdash.json) joins the Gold outputs for reporting.

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