# [ADR-002] Silver Layer Strategy: Streamlining to Relational DWH

**Date:** 2025-12-10
**Status:** Finalized
**Related Files:** `src/transform/alpha_transform.py`, `src/load/db_load_silver.py`, `dags/alpha_dag.py`

### Decision

The Silver Layer will be implemented using a **Streamlining to Relational DWH** approach, where the canonical destination of the Transform and Load (T/L) stage will be **exclusively the PostgreSQL database**.

The Polars DataFrame will be loaded directly from memory into the `stocks.weekly_adjusted_stocks` table in PostgreSQL. The **redundant write operation** to S3 in Parquet format is eliminated for the Alpha Vantage dataset.

### Objectives

1. **Prioritize Efficiency and Cost Tractability:** Eliminate the complexity and operational overhead associated with a dual-write mechanism, which is unwarranted for the volume and consumption needs of this specific dataset.
1. **Optimize the ELT Flow:** Simplify the pipeline, ensuring the flow $\\text{Bronze (S3 JSON) } \\xrightarrow{\\text{Polars T}} \\text{PostgreSQL L}$ operates with maximum velocity and minimal complexity.
1. **Reinforce DWH Modeling:** Establish PostgreSQL as the high-performance, **sole source of relational truth** for dbt's dimensional modeling within the Gold Layer.

### Technical Justification

- **Principle of Efficiency:** For the low-to-moderate volume of the Alpha Vantage dataset, the operational cost (runtime, complexity, maintenance overhead) of a dual-write strategy significantly outweighs the architectural benefits.
- **Avoiding Redundancy:** Since this dataset does not require ad-hoc Data Lake analytics or advanced Machine Learning (which would justify S3 Parquet storage), the PostgreSQL Silver Layer is the only artifact required to support downstream dimensional modeling.
- **Portfolio Focus:** This decision focuses the architecture demonstration on established **Data Warehouse/ELT best practices (Airflow, Polars, dbt)**, deferring a Lakehouse implementation to a future project where volume or mutation requirements explicitly justify the complexity (as outlined in ADR-003).

### Code Impact

- The dependency and call to the redundant `load_silver_to_s3()` function in `src/transform/alpha_transform.py` were removed.
- The codebase is streamlined; the `transform_load_to_silver` task now solely manages the canonical call to `db_load_silver.load_dataframe_to_db()`.

______________________________________________________________________

## 🧭 Confirmed Next Step

With the architectural step documented, we will continue with the **dbt Project Configuration** for the Gold Layer (Dimensional), which will consume the data from its Silver Layer in PostgreSQL.
