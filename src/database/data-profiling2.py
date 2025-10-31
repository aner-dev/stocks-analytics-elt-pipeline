# realistic_profiling.py
import duckdb

csv_path = "/tmp/tmpzm5bjkbf.csv"
conn = duckdb.connect()

# Solo 2 queries para toda la info esencial
print("=== SCHEMA ===")
print(conn.sql(f"DESCRIBE SELECT * FROM '{csv_path}'"))

print("\n=== BASIC STATS ===")
print(
    conn.sql(f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT supplier) as unique_suppliers,
        COUNT(DISTINCT "ITEM CODE") as unique_items
    FROM '{csv_path}'
""")
)

conn.close()
