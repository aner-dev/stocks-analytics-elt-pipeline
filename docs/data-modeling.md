# 📂 Logical Data Modeling Document

**Last Updated:** 2025-12-27
**Status:** ✅ Final (Post-Gold Refactor)

## 🏗️ Design Principles
- **Naming Convention:** `snake_case` for all entities.
- **Metadata Standard:** Prefix `_` for lineage/technical columns (e.g., `_ingestion_timestamp`).
- **Architecture:** Medallion Architecture (Silver -> Gold).

---

## 🥈 Silver Layer: Staging & Clean Truth
*The `stocks` schema acts as the unique source of truth.*

### 1. Primary Entity: `stocks.weekly_adjusted_stocks`
- **Granularity:** 1 record per Symbol per Week.
- **Integrity:** Compound PK on `(symbol, trade_date)`.
- **Key Columns:** `trade_date`, `adjusted_close_price`, `trade_volume`.
- **Lineage:** `_ingestion_timestamp`, `_data_source`, `_processing_date`.

---

## 🥇 Gold Layer: Dimensional Modeling (Star Schema)
*Managed by dbt. Optimized for High-Performance BI (Streamlit).*

### 1. Fact Table: `fact_adjusted_prices`
| Column | Type | Rationale |
| :--- | :--- | :--- |
| `stock_id` | UUID | Surrogate Key (MD5). |
| `symbol` | TEXT | Denormalized for fast filtering. |
| `company_name` | TEXT | Enriched via dbt Seeds. |
| `weekly_return_pct` | NUMERIC | Business Metric (SQL calculated). |
| `volatility_pct` | NUMERIC | Risk Metric (Handles Splits). |

### 2. Dimensions
- **`dim_stock`**: Enriched master data (Sector, Industry).
- **`dim_date`**: Time intelligence (Week number, Year).

---

## 📉 Summary of Refactor Decisions
1. **Denormalization:** We brought `company_name` into the Fact table to eliminate JOIN latency in the UI.
2. **Logic Centralization:** Moved financial calculations from Python to dbt (SQL) for a single source of truth.
3. **Data Quality:** Tuned `dbt_expectations` to handle historical stock splits (Threshold 10.0).
