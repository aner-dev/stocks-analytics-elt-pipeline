# 📈 ELT Pipeline for Stock Market Analysis (Batch) - Dimensional Data Warehouse

## 📄 Project Overview and Summary

This project implements a robust and modular **ELT (Extract, Load, Transform) Pipeline** designed to ingest, clean, and model stock price data into a **Dimensional Data Warehouse (Star Schema)**.

The solution leverages the popular integration of **Apache Airflow**, **dbt**, and **Cosmos** for reliable orchestration and high-quality data transformations. The focus is on **Batch Processing**, utilizing industry-leading tools to ensure data quality and performance in preparing information ready for consumption by Business Intelligence (BI) tools.

## 🎯 Objective and Architecture

### **1. Core Architectural Flow**

The pipeline is structured using a three-layer data design (Bronze, Silver, Gold), orchestrated through two sequential Airflow DAGs.

```mermaid
graph TD
    A[Source: External Stock API] --> B(Ingestion / E);
    B --> C{Bronze Layer: Raw Data};
    C --> D[Airflow DAG 1: Silver Processing];
    D --> E(Polars: Cleaning, Casting, Keys);
    E --> F{Silver Layer: Cleaned Data in DWH};
    F --> G[Airflow DAG 2: Gold Modeling (Cosmos)];
    G --> H(dbt: Dimensional Modeling);
    H --> I{Gold Layer: Star Schema (fact_prices, dim_symbol)};
    I --> J[BI / Analytics];

    style A fill:#f9f,stroke:#333
    style I fill:#ccf,stroke:#333
```
