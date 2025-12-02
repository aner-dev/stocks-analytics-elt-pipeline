CREATE OR REPLACE VIEW nyc_taxi_trips AS
SELECT * FROM read_parquet('data/nyc_taxi/raw/*.parquet');
SELECT
    extract(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
    avg(total_amount / trip_distance) AS avg_fare_per_mile
FROM
    nyc_taxi_trips
WHERE
    trip_distance > 0.1 AND total_amount > 0
GROUP BY
    pickup_hour
ORDER BY
    avg_fare_per_mile DESC;
