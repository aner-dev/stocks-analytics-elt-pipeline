import duckdb
import structlog
import os

log = structlog.get_logger()

# URL del archivo en S3 /S3 (una vez que lo hayas subido)
# Para el Data Discovery inicial, puedes usar la URL pública de ClickHouse
# Asegúrate de que DuckDB tiene permisos para leer S3 (si usas S3 ).

# URL pública para el ejemplo
HN_DATA_URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.csv.gz"


def run_data_discovery(url: str):
    log.info("Starting DuckDB Data Discovery on compressed CSV file.")

    # 1. Conexión a DuckDB (modo en memoria para el discovery)
    con = duckdb.connect()

    # 2. Ejecutar la inspección del esquema (Simulando DESCRIBE TABLE)
    # DuckDB puede leer directamente archivos remotos y comprimidos (.gz)
    schema_query = f"""
    DESCRIBE SELECT *
    FROM read_csv_auto('{url}', ALL_VARCHAR=TRUE)
    """

    log.info("Running DESCRIBE on the remote file...")
    schema_df = con.sql(schema_query).df()
    print("\n--- INFERRED SCHEMA ---")
    print(schema_df)
    print("-----------------------\n")

    # 3. Ejecutar una consulta de muestra
    sample_query = f"""
    SELECT
        type,
        COUNT(id) AS item_count,
        MIN(score),
        MAX(score)
    FROM read_csv_auto('{url}')
    GROUP BY 1
    ORDER BY 2 DESC;
    """
    log.info("Running sample aggregation query...")
    sample_df = con.sql(sample_query).df()
    print("\n--- SAMPLE AGGREGATION ---")
    print(sample_df)
    print("--------------------------\n")

    con.close()
    log.info("Data Discovery complete.")


if __name__ == "__main__":
    # Si la data ya está en S3 /Bronze, usa la URL de S3 
    # Si no, usa la URL pública para el discovery inicial
    run_data_discovery(HN_DATA_URL)
