from download_csv import download_csv
from loguru import logger


def analyze_null_suppliers(csv_path: str):
    import duckdb

    conn = duckdb.connect()

    result = conn.execute(f"""
        SELECT 
            COUNT(*) as null_suppliers,
            AVG("RETAIL SALES") as avg_sales_when_null,
            COUNT(DISTINCT "ITEM TYPE") as item_types_affected,
            MIN(year) as earliest_year,
            MAX(year) as latest_year
        FROM '{csv_path}' 
        WHERE supplier IS NULL
    """).fetchone()

    logger.info(f"🔍 NULL Suppliers Analysis:")
    logger.info(f"   Total: {result[0]}")
    logger.info(f"   Avg Sales: ${result[1]:.2f}")
    logger.info(f"   Item Types Affected: {result[2]}")
    logger.info(f"   Date Range: {result[3]}-{result[4]}")

    conn.close()


if __name__ == "__main__":
    csv_path = download_csv("data", "extracted_data.csv")
    analyze_null_suppliers(csv_path)
