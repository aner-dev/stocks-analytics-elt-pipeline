# ELT workflow
- extract the data with python script from a API 
- load that data into a bucket (s3/minIO)
- transform using *dataframes*
  - duckdb through sql code, for cleaning and adapting raw data to the star schema of the postgresql database 
  - here is where are applied: WITH CTE, window functions, etc 
  - star schema: OLAP queries 
- BI/visualization: apache superset for do BI queries of the already transformed data 
