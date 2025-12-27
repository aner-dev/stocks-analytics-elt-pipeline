# [ADR-003] Enrichment Strategy: dbt Seeds vs. API Metadata Extraction

**Date:** 2025-12-27
**Status:** Accepted
**Related Files:** `seeds/stock_metadata.csv`, `models/gold/dim_stock.sql`, `models/gold/fact_adjusted_prices.sql`

### Context
The current Gold Layer produces a `dim_stock` table where company names, sectors, and industries are marked as "Unknown". While the Alpha Vantage "Weekly Adjusted" API provides price series, it does not include descriptive metadata. To deliver a functional dashboard in Streamlit, we need to resolve these attributes to allow for filtering and grouping by sector.

### Decision
We will implement **dbt Seeds** as the primary mechanism for stock metadata enrichment. This involves maintaining a version-controlled CSV file within the dbt project and performing a `LEFT JOIN` during the materialization of the `dim_stock` model. 

We explicitly reject the immediate implementation of a secondary API extraction flow (Endpoint `OVERVIEW`) for this stage of the project.

### Technical Justification

1.  **API Quota Management & Rate Limiting:** Alpha Vantage's free tier is highly restrictive (5 RPM). Adding a metadata call for every symbol would double the execution time of the extraction phase and increase the surface area for `429 Too Many Requests` errors.
2.  **Latency & Computational Cost:** Joining with a local seed file (already loaded into PostgreSQL via `dbt seed`) is an $O(1)$ or $O(log n)$ operation in the database, whereas an API-based enrichment adds network I/O latency and requires additional "Silver" transformation logic.
3.  **Data Stability (Master Data Management):** Company names and sectors are "Slowly Changing Dimensions" (SCD Type 0/1). They are essentially static for our current scope. Orchestrating a dynamic extraction for static attributes violates the **YAGNI (You Ain't Gonna Need It)** principle.
4.  **Portfolio Focus:** Implementing seeds demonstrates proficiency in dbt's multi-source integration capabilities, showing a balanced approach between dynamic (API) and static (Seed) data ingestion.

### Consequences
* **Pros:** Immediate availability of rich metadata, zero additional API cost, and simplified SQL logic in the Gold layer.
* **Cons:** New symbols added to the pipeline in the future will require a manual entry in the `stock_metadata.csv` file to avoid "Unknown" values.
