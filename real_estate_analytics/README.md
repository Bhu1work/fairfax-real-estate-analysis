# Fairfax County Real Estate Analysis Pipeline

## Project Goal
The objective of this project was to build a complete, end-to-end ELT (Extract, Load, Transform) data pipeline using modern, enterprise-grade tools. The pipeline ingests live real estate sales data from the Fairfax County public API, cleans and models it using dbt, and presents the insights in an interactive Business Intelligence dashboard.

## Technology Stack
* **Language:** Python
* **Data Ingestion:** `requests`, `pandas`
* **Data Warehouse:** PostgreSQL
* **Data Transformation & Testing:** `dbt`
* **BI & Visualization:** Microsoft Power BI
* **Version Control:** Git / GitHub

## The Pipeline
1.  **Extract & Load:** A Python script (`scripts/ingest_data.py`) connects to the live API, extracts data, and loads it into a `raw` schema in a PostgreSQL database.
2.  **Transform:** `dbt` models read from the raw data, perform cleaning, casting, and renaming, and materialize the results as a clean view in a `production` schema.
3.  **Test:** `dbt test` runs a suite of data quality checks (e.g., `not_null`, `unique`) to ensure the integrity of the transformed data.
4.  **Visualize:** A Power BI dashboard connects directly to the clean `production` view in PostgreSQL to provide interactive analytics.

## How to Run This Project
1.  Clone the repository.
2.  Set up a PostgreSQL database.
3.  Install dependencies: `pip install -r requirements.txt`
4.  Configure your dbt profile (`profiles.yml`) with your database credentials.
5.  Run the ingestion script: `python scripts/ingest_data.py`
6.  Run the dbt models and tests: `dbt run` followed by `dbt test`.
7.  Connect Power BI to the `production` schema in your PostgreSQL database.

## Dashboard Screenshot
*(It's a great idea to embed a screenshot of your final Power BI dashboard here!)*