# Projects Portfolio

A compact, projects portfolio that demonstrates ETL in Azure, PySpark lakehouse patterns, production grade Python ETL with tests, advanced SQL for data management, and Power BI measures and build steps.

Created: 2025-09-04

## Structure

1. azure
2. python_etl
3. sql
4. powerbi
5. data

## The flow for audience

1. Start with `azure/databricks/bronze_silver_gold_etl.py` to show lakehouse patterns with Delta. Walk through Bronze to Silver to Gold and explain incremental loads and schema evolution.
2. Open `python_etl/README.md` and run the light ETL into SQLite. Show configuration, logging, tests, and simple orchestration.
3. Open `sql/complex_queries.sql` and highlight three queries: Slowly Changing Dimension Type 2, data quality checks, and a window function example.
4. Open `powerbi/README.md`. Explain the star schema, the measures in `powerbi/measures.dax`, and how to build the three page report using the provided CSV files in `data`.


## Attribution

Synthetic sample data. No proprietary content.
