# High-Performance Stock Analytics Pipeline

This project demonstrates an end-to-end ELT pipeline for financial data processing, utilizing **Polars** for high-speed ingestion and **dbt** for sophisticated data modeling.

## Table of contents

- [Overview](#overview)
- [The Goal](#the-goal)
- [The Dataset](#the-dataset)
- [Data Modeling](#data-modeling)
- [Tools](#tools)
- [Key Engineering Challenges](#key-engineering-challenges)
- [Running the project](#running-the-project)
  - [1. Requirements](#1-requirements)
  - [2. Clone the repository](#2-clone-the-repository)
  - [3. Environment Setup](#3-environment-setup)
  - [4. Run Airflow & Postgres](#4-run-airflow--postgres)
  - [5. The ELT Workflow](#5-the-elt-workflow)
  - [6. Data Visualization](#6-data-visualization)
- [Project Limitations](#project-limitations)

## Overview

The current work aims to provide actionable insights into stock market trends and performance metrics. We have built a data pipeline that extracts raw financial data from external APIs, stores it in a Data Lake, and applies multi-layered transformations to feed a Star Schema in a Data Warehouse.

The following diagram illustrates the high-level architecture:

![The ELT Pipeline](/images/architecture_diagram.png "Architecture: API -> S3 -> Polars -> Postgres -> dbt")

## The Goal

The end goal is to preprocess financial data and derive useful metrics to answer business questions such as:

- What is the weekly adjusted performance trend for specific technology tickers?
- How does trading volume correlate with price volatility over a 52-week period?
- Which stocks are showing consistent dividend growth patterns?

## The Dataset

We process financial time-series data retrieved from **Alpha Vantage API**:

1. __Weekly Adjusted Stock Prices__: Includes open, high, low, close, adjusted close, volume, and dividend history.
1. __Tickers Metadata__: A collection of global symbols (e.g., AAPL, NVDA, TSCO.LON) mapped dynamically through Airflow tasks.

Data is extracted as raw JSON and stored in a **Rust-based S3 compatible storage (RustFS)** before being processed.

## Data Modeling

We follow the **Medallion Architecture** (Bronze, Silver, Gold) and a final **Star Schema** for the Data Warehouse.

![The ERD](/images/stocks_erd.png "Entity Relationship Diagram")

- **Bronze Layer**: Raw JSON payloads stored in S3.
- **Silver Layer**: Normalized tables in Postgres (cleaned via Polars).
- **Gold Layer**: Dimensional models (`dim_stock`, `dim_date`) and Fact tables (`fact_weekly_prices`) managed by dbt.

## Tools

1. **Polars**: A blazingly fast Dataframe library written in Rust. Used for the Silver layer transformation to handle high-concurrency ingestion.
1. **Apache Airflow (Astro SDK)**: Orchestrates the entire lifecycle, using **Dynamic Task Mapping** to process multiple tickers in parallel.
1. **dbt (data build tool)**: Handles the T (Transform) in our ELT, creating materialized views and tables in the Gold layer.
1. **Astronomer Cosmos**: Integrates dbt directly into Airflow, allowing us to see dbt models as native Airflow tasks.
1. **PostgreSQL**: Our primary Data Warehouse, running in a containerized environment.
1. **RustFS (S3 API)**: Used as a local Data Lake for storing raw JSON extracts.
1. **Podman/Docker**: Containerization for all services (Postgres, Airflow).
1. **Evidence.dev**: (In Progress) BI-as-code tool to visualize data using SQL and Markdown.

## Key Engineering Challenges

- **API Rate Limiting**: Implemented a robust extraction library with backoff logic and session management to respect Alpha Vantage's limits.
- **Dynamic Schema Resolution**: Created custom dbt macros to prevent schema naming collisions when orchestrating through Cosmos.
- **Parallel Processing**: Leveraged Airflow's `.expand()` to map API calls and transformations across multiple symbols simultaneously.

## Running the project

### 1. Requirements

- **Artix Linux** (or any Linux distro) with **Podman/Docker**.
- **Astro CLI** installed.
- **Alpha Vantage API Key**.

### 2. Clone the repository

```bash
git clone [https://github.com/youruser/elt_pipeline_stocks.git](https://github.com/youruser/elt_pipeline_stocks.git)
cd elt_pipeline_stocks
```
