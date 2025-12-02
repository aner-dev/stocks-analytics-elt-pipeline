import duckdb

conn = duckdb.connect()

conn.execute("""
CREATE OR REPLACE VIEW nyc_taxi_trips AS
SELECT * FROM read_parquet('data/nyc_taxi/raw/*.parquet')
""")

result = conn.execute("""
    SQL 
    SELECT f.tpep_pickup_datetime, d.neighborhood_name FROM fact_taxi_trips AS f 
        WHERE f.trip_distance > 10 
    AND f.total_amount > 30 
    AND d.borough = 'Manhattan' 
    INNER JOIN dim_locations d ON f.PULocationID = d.location_id 

""").fetchall()
print("average fer per mile for day hour")
for row in result:
    print(f"hour {row[0]:02d}: ${row[1]:.2f} per mile")


conn.close()
