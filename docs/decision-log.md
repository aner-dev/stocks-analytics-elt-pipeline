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
-
