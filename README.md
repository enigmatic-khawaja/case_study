# Sales Data ETL Pipeline

## Overview
This project implements a robust, production-grade ETL (Extract, Transform, Load) pipeline for sales data using PySpark. The pipeline ingests, cleans, enriches, and loads sales and product data into a Microsoft SQL Server database, with full support for error logging, currency conversion, and detailed monitoring.

## Features
- **PySpark-based ETL**: Scalable data processing for large datasets.
- **Data Cleaning & Validation**: Handles nulls, duplicates, schema validation, and business rules.
- **Data Enrichment**: Product lookup and currency conversion with API integration and caching.
- **Structured Logging**: Separate logs for pipeline, errors, and currency conversion.
- **SQL Server Integration**: Automated schema creation, data loading, and metrics.
- **Jupyter Notebook Support**: Interactive pipeline execution and exploration.
- **Dockerized**: One-command setup for all dependencies and services.

## Architecture
- **src/**: All ETL modules (ingestion, cleaning, enrichment, loading, utils).
- **data/**: Raw and processed data files.
- **logs/**: All pipeline, error, and currency logs.
- **config.yaml**: Central configuration for pipeline, validation, and database.
- **notebooks/**: Jupyter notebooks for interactive use.
- **sql/**: SQL scripts for schema and queries.

## Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop) (Windows/Mac/Linux)
- (Optional) Python 3.8+ if running locally

## Quick Start (Recommended: Docker Compose)
1. **Clone the repository:**
   ```sh
   git clone <repo-url>
   cd case_study
   ```
2. **Start all services:**
   ```sh
   docker-compose up -d
   ```
   This will start:
   - Microsoft SQL Server (on port 1433)
   - Jupyter Notebook (on port 8888)

3. **Access Jupyter Notebook:**
   - Open [http://localhost:8888](http://localhost:8888) in your browser.
   - Use token: `etlpipeline`
   - Open the main notebook and run cells to execute the ETL pipeline.

4. **Check Logs and Outputs:**
   - Logs: `logs/` directory (pipeline, errors, currency)
   - Processed data: `data/processed/`
   - Database: Connect to SQL Server at `localhost:1433` (user: `sa`, password: `YourStrong@Passw0rd`)

## Configuration
- **config.yaml**: Central config for database, data sources, validation, logging, and processing. max_error_rate is set to 15% for running the pipeline if set to 5% it will fail. 
- **Edit as needed** for your environment or business rules.

## Troubleshooting
- **SQL Server connection errors**: Ensure Docker has at least 4GB RAM allocated.
- **FileNotFoundError**: Make sure `config.yaml` and `data/` are present and mounted in the container.
- **Login failed for user 'sa'**: The pipeline will auto-create the database if missing. If issues persist, restart containers.
- **Logs not updating**: Check file permissions and Docker volume mounts.

## Extending the Pipeline
- Add new data sources or validation rules in `src/data_ingestion.py` and `config.yaml`.
- Add new enrichment logic in `src/data_enrichment.py`.
- Update schema or database logic in `src/utils/database.py` and `sql/`.
