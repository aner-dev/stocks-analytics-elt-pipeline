import duckdb



def explore_nyc_taxi_schema():
    """Descubre automáticamente el schema de NYC Taxi data"""

    conn = duckdb.connect()
    DATA_PATH = (
        "data/nyc_taxi/raw/yellow_tripdata_2024-01.parquet"  # Un archivo de ejemplo
    )

    print("🔍 EXPLORING NYC TAXI SCHEMA")
    print("=" * 50)

    # 1. Descubrir columnas y tipos
    print("📋 COLUMNS & DATA TYPES:")
    schema = conn.execute(f"""
        DESCRIBE SELECT * FROM read_parquet('{DATA_PATH}')
    """).fetchall()

    for col_name, col_type, null, key, default, extra in schema:
        print(f"   {col_name:<25} {col_type}")

    # 2. Ver sample de datos
    print(f"\n📊 SAMPLE DATA (first 5 rows):")
    sample = conn.execute(f"""
        SELECT * FROM read_parquet('{DATA_PATH}') LIMIT 5
    """).fetchall()

    for i, row in enumerate(sample):
        print(f"   Row {i + 1}: {row}")

    # 3. Estadísticas básicas de columnas
    print(f"\n📈 COLUMN STATISTICS:")
    stats = conn.execute(f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT VendorID) as unique_vendors,
            MIN(tpep_pickup_datetime) as earliest_trip,
            MAX(tpep_pickup_datetime) as latest_trip,
            AVG(trip_distance) as avg_distance,
            AVG(total_amount) as avg_fare
        FROM read_parquet('{DATA_PATH}')
    """).fetchone()

    print(f"   Total rows: {stats[0]:,}")
    print(f"   Unique vendors: {stats[1]}")
    print(f"   Date range: {stats[2]} to {stats[3]}")
    print(f"   Avg distance: {stats[4]:.2f} miles")
    print(f"   Avg fare: ${stats[5]:.2f}")

    conn.close()


if __name__ == "__main__":
    explore_nyc_taxi_schema()
