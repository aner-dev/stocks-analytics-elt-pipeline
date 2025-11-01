# transfomation 
- apply sql complex queries for data modeling *before* or *after* the load into the db? 
- the question that response that is: what the enterprise or client actually need? 
  - A: scalability & performance compute 
  - B: data governance & maintaining of the data 
- FAANG example:
  - the *petabytes* (big data) will saturate a traditional relational warehouse 
   - the heavy transform will be executed across *distributed systems* (clusters - sparks/databricks)
    - and only the final result would be loaded into the warehouse (snowflake/redshift e.g.)
  - synthesis: the heavy and expensive logic are executed where the computing is more cheap and scalable
## API requests 
- alpha vantage API restriction: 1 symbol per API call 
## minIO buckets 
- actual: 'stocks-raw'. 
- implement: 'stocks=curated'
  - casting data into Parquet for read optimization and organized by partitions
- 
