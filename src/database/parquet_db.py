# direct_copy.py
import duckdb


def direct_copy_to_postgres():
    """COPY directo desde DuckDB a PostgreSQL"""

    conn = duckdb.connect()

    # COPY directo a PostgreSQL
    conn.execute("""
        COPY (
            SELECT * FROM read_parquet('data/nyc_taxi/raw/*.parquet')
        ) TO 'postgresql://aner:aner@localhost:5432/nyc_taxi_db' 
        WITH (TABLE 'taxi_trips');
    """)

    print("🎉 Data copied directly to PostgreSQL!")


if __name__ == "__main__":
    direct_copy_to_postgres()
