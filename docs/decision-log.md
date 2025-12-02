# focus change to alpha vantage API 
- proyect evolutions from static dataset (data.gov) for a constant ingest of data  
  - reasons: find limitations when trying to expand the scope of the project 

# extract data
- extract.py created for dataset from data.gov 
- api.env & db.env 
  - management with environment variables 
- db_connection.py 
  - implementing manual connection to PostgreSQL database
## benchmarking
- polars choosed for performance optimization at reading the csv
  - refer to benchmarking.md for implementation details
## documentation 
- switched from mkdocs to mkdocs-material 
# why use minIO 
- what problem i solve using a minIO bucket? 
- reasons for incorporate a minIO bucket in the architecture: 
- re-processing: if Transform logic changes, re-read from minIO without call API again 
MinIO acts as a resilient staging layer, ensuring data durability and operational reliability.

1. Fault Tolerance & Recovery
If transformation fails, raw data persists in MinIO
Enables reprocessing without API re-calls

2. Data Quality & Debugging
Keep original data snapshots for validation
Compare raw vs transformed data for debugging

3. Historical Tracking
Maintain versioned raw data for audit trails
Support historical reprocessing when business logic changes

4. Pipeline Resilience
Decouples extraction from transformation
Prevents data loss during pipeline failures
Enables backfilling and batch reprocessing

# docker 
- what problem i solve using docker compose? 
- docker engine
# transformation & data types 
- postgreSQL can't read efficiently .parquet files to fill their tables 
- because .parquet is *column oriented* and psql is row-oriented 
- however, the temporal type transformation to parquet in T1 is a worth-it trade-off 
- duckdb & polars are columnar engines; reading Parquet is significantly faster and cheaper than reading CSV for complex transformations.
## monitoring 
- added monitoring through metadata dictionaries 
- improvements: 
  - better interaction between pipeline and monitoring systems & tools (like Graphana, Prometheus)
  - better logging 








