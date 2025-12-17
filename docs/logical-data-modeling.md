name convention: snake_case & prefix for metadata (\_ingestion_timestamp)

# silver layer main goal

- serve as unique and clean source of truth for dbt (data-build-tool) and posterior creation of gold layer (dimensional modeling)

# transactional data (main entity)

- granularity: a register for symbol, week/date
- keys: CK/compose key (timestamp, symbol)
  - ensure entity integrity
- data types: mapping JSON data to native PostgreSQL data types
- data lineage (metadata):
  - ingestion of columns: \_ingestion_timestamp, \_data_source, \_processing_date
  - guaranteeing observability and auditability of silver layer
    - allowing tracking of origin and data freshness

# auditory table (observability)

- table: stocks.pipeline_audit
- entity purpose: snapshot/register type
- separates business logic (prices data) of control/system logic (auditory)
- key metrics: rows_processed, rows_inserted, rows_rejected & duration_seconds columns/fields

# 📂 Logical Data Modeling Document - Silver Layer

**Date:** 2025-12-10
**Scope:** Definition of the `stocks` schema and the Silver Layer tables (dbt Source).

______________________________________________________________________

## 🎯 Primary Goal of the Silver Layer

The Silver Layer (`stocks` schema) is the **final staging area** and the **single source of clean truth** for the Gold Layer (Dimensional Modeling) transformations executed by dbt.

### Implemented Conventions:

- **Schema:** Separation of the `stocks` schema from the `public` schema for organization and permission management.
- **Naming:** The **`snake_case`** convention is used for all table and column names (e.g., `adjusted_close`, `rows_inserted`).
- **Metadata:** A prefix (`_`) is used to identify audit and lineage columns (e.g., `_ingestion_timestamp`).

______________________________________________________________________

## 1. 📈 Primary Entity: Transactional Stock Data

**Table:** `stocks.weekly_adjusted_stocks` (Silver Layer)

| Logical Aspect | Implementation | Rationale / Purpose |
| :--- | :--- | :--- |
| **Granularity** | One record per **Stock Symbol (Ticker)** and **Date/Week**. | Atomic level of detail, ideal for serving as a base (source) for future Fact Tables. |
| **Primary Key (PK)** | Compound Key (CK) over **`(timestamp, symbol)`**. | Ensures **Entity Integrity** (unicity) for each weekly observation. |
| **Data Types** | Direct mapping from JSON/Polars data to native PostgreSQL types. | **Domain Integrity.** Use of `DATE`, `DOUBLE PRECISION` (for prices), and `BIGINT` (for volume). |
| **Lineage and Metadata** | Columns: `_ingestion_timestamp`, `_data_source`, `_processing_date`. | Ensures **Auditability and Traceability** (Data Lineage), allowing the measurement of data freshness and origin. |

______________________________________________________________________

## 2. 🛡️ Support Entity: Audit Table (Observability)

**Table:** `stocks.pipeline_audit`

| Logical Aspect | Implementation | Rationale / Purpose |
| :--- | :--- | :--- |
| **Entity Purpose** | **Log / Snapshot** type entity for each pipeline execution. | Separates **Control/System** logic from **Business** logic (price data). |
| **Key Metrics** | Stores `rows_processed`, `rows_inserted`, `rows_rejected`, and `duration_seconds`. | Allows calculation of the **Service Level Agreement (SLA)** and **Data Quality** (rejection rate) for each E/T/L task. |
| **Execution** | A record is inserted upon successful completion of the Polars DataFrame load. | Ensures **Observability**, providing task-level metrics for Airflow. |

______________________________________________________________________

## ➡️ Next Step (Dimensional Modeling)

The Logical Modeling defines the source structure (Silver Layer). The next phase, the **Dimensional Modeling (Star Schema)**, will use this Silver Layer to create Dimensions and Fact Tables for the Gold Layer.

______________________________________________________________________

Shall we proceed with the **dbt Project Configuration** for the Gold Layer?
