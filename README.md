# Cubix Data Engineer Capstone

## Project Overview

This project demonstrates an end-to-end data engineering pipeline built on Databricks (Free Edition) using a Medallion Architecture approach.

Starting from six simple CSV source files:

- calendar
- customers
- product_category
- product_subcategory
- products
- sales

the pipeline ingests, cleans, transforms, and models the data across Bronze, Silver and Gold layers, ultimately producing analytics-ready tables.

### Final Outputs

- One Big Table (OBT)
  - wide_sales
- Dashboard-ready aggregate tables
  - daily_product_category_metrics
  - daily_sales_metrics

Dashboard creation is intentionally out of scope - the focus of this repository is data engineering.

## Architecture

The pipeline follows a classic Medallion Architecture pattern.

### Bronze
- Raw CSV ingestion

### Silver
- Business logic application
- Deduplication
- Type casting
- SCD Type 1 logic for dimension tables
- Star and snowflake schema modeling

### Gold
- Analytics-ready tables
- Aggregations for reporting
- One Big Table (OBT) creation

All tables are stored using Delta Lake.

## Technologies Used

- Python
- PySpark
- Spark SQL
- Delta Lake
- Databricks (Free Edition – no cloud backend)
- Git

## Concepts Applied

- Medallion Architecture (Bronze / Silver / Gold)
- Slowly Changing Dimensions (SCD Type 1)
- Star schema and snowflake schema
- One Big Table (OBT) design
- Unit testing for data transformations
- Data quality validation

## Running the Project on Databricks

The project was developed locally using VS Code with Git integration, but the pipeline runs on Databricks.

High-level steps:

1. Create the repository locally.
2. Build or package the project from VS Code.
3. Import the package into a Databricks notebook.
4. Configure two Databricks volumes:
   - One for source CSV data
   - One for the project (capstone) data
5. Run the pipeline notebooks:
   - Bronze ingestion
   - Silver transformations
   - Gold aggregations
6. Validate data quality using Great Expectations at the end.

Designed to run on Databricks Free Edition without cloud storage integration.

## Installing Dependencies

Dependencies are managed using Poetry and defined in pyproject.toml.

At a high level, the project requires:
- Python 3.11+
- PySpark
- NumPy

Optional dependency:
- Delta Lake via delta-spark

Development and testing tools:
- pytest
- pandas
- pyarrow
- pre-commit

## Running Tests

Unit tests are implemented using pytest and focus on transformation logic in the Silver and Gold layers.

### Testing approach

- Test data is created as Spark DataFrames
- Transformation functions are applied
- Results are compared against expected DataFrames

Assertions are performed using: pyspark.testing.assertDataFrameEqual
