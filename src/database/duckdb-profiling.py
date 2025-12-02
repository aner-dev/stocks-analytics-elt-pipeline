import duckdb
from elt.config.logging_config import get_log.
log.= get_log.__name__)
 
log.= get_log.__name__)



def enhanced_profile(csv_path: str):
    conn = duckdb.connect()

    log.info("🔍 BUSCANDO ISSUES DE CALIDAD...")

    # 1. Valores nulos por columna
    null_check = conn.execute(f"""
        SELECT 
            COUNT(*) - COUNT(year) as null_year,
            COUNT(*) - COUNT(month) as null_month, 
            COUNT(*) - COUNT(supplier) as null_supplier,
            COUNT(*) - COUNT("ITEM CODE") as null_item_code
        FROM '{csv_path}'
    """).fetchone()

    log.info(
        f"🚨 VALORES NULOS: year={null_check[0]}, month={null_check[1]}, supplier={null_check[2]}, item_code={null_check[3]}"
    )

    # 2. Valores fuera de rango
    range_check = conn.execute(f"""
        SELECT 
            COUNT(CASE WHEN year < 2000 OR year > 2030 THEN 1 END) as invalid_year,
            COUNT(CASE WHEN month < 1 OR month > 12 THEN 1 END) as invalid_month,
            COUNT(CASE WHEN "RETAIL SALES" < 0 THEN 1 END) as negative_sales
        FROM '{csv_path}'
    """).fetchone()

    log.info(
        f"📏 VALORES FUERA DE RANGO: años={range_check[0]}, meses={range_check[1]}, ventas_negativas={range_check[2]}"
    )

    # 3. Duplicados exactos (sintaxis corregida para DuckDB)
    duplicates = conn.execute(f"""
        WITH all_rows AS (
            SELECT COUNT(*) as total FROM '{csv_path}'
        ),
        distinct_rows AS (
            SELECT COUNT(*) as distinct_count FROM (
                SELECT DISTINCT * FROM '{csv_path}'
            )
        )
        SELECT total - distinct_count as exact_duplicates
        FROM all_rows, distinct_rows
    """).fetchone()[0]

    log.info(f"🔄 DUPLICADOS EXACTOS: {duplicates}")

    conn.close()


if __name__ == "__main__":
    enhanced_profile("/tmp/tmpzm5bjkbf.csv")
