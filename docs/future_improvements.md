# airflow pools

To scale this for multiple DAGs sharing the same API quota, I would implement an Airflow Pool with 1 slot to prevent 429 errors across the entire cluster.
