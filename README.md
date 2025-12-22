# 📈 ELT Pipeline for Stock Market Analysis (Batch) - Dimensional Data Warehouse

## 📄 Project Overview and Summary

This project implements a robust and modular **ELT (Extract, Load, Transform) Pipeline** designed to ingest, clean, and model stock price data into a **Dimensional Data Warehouse (Star Schema)**.

The solution leverages the popular integration of **Apache Airflow**, **dbt**, and **Cosmos** for reliable orchestration and high-quality data transformations. The focus is on **Batch Processing**, utilizing industry-leading tools to ensure data quality and performance in preparing information ready for consumption by Business Intelligence (BI) tools.

## 🎯 Objective and Architecture

### 🏗 Data Pipeline Architecture

```mermaid
graph TD
    subgraph External_Sources
        AV[Alpha Vantage API]
    end

    subgraph Infrastructure_Docker
        RFS[(RustFS S3 Storage)]
        DWH[(PostgreSQL: stocks_dwh)]
    end

    subgraph DAG_Silver_Layer [Orchestrated by Airflow]
        E[Extract: Python] --> V[Validate: Data Quality]
        V --> L[Load Bronze: JSON in RustFS]
        L --> T[Transform: Polars]
        T --> S[Load Silver: Postgres]
    end

    subgraph DAG_Gold_Layer [Orchestrated by Cosmos/dbt]
        G1[dbt Models: Star Schema]
        G2[dbt Tests: Expectations]
    end

    AV --> E
    L -.-> RFS
    S -.-> DWH
    S -- "Trigger: STOCKS_SILVER_DATASET" --> DAG_Gold_Layer
    DWH --> G1
```
