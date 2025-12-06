import requests
import structlog

log = structlog.get_logger()

# NOTE: The existing functions (like extract_stocks_data, symbols_list)
# and environment variable references (BASE_URL, API_KEY) should remain
# in this file if you are keeping the stocks pipeline.


# extract
def extract_hn_data_bytes(url: str) -> bytes:
    """
    Downloads the binary .csv.gz file from the public HTTP/S URL.
    Returns the raw bytes for direct loading into the Bronze layer.

    Args:
        url (str): The public URL to the compressed data file.

    Returns:
        bytes: The raw content of the .csv.gz file.

    Raises:
        requests.RequestException: If the HTTP request fails.
    """
    try:
        log.info("Starting large file download from URL", url=url)

        # Set a generous timeout (5 minutes or 300 seconds) for large file downloads.
        # Stream=True is not strictly necessary here but is good practice for large files.
        response = requests.get(url, timeout=300, verify=True)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

        # Return the binary content directly without decoding (it's a .gz file)
        raw_bytes = response.content
        log.info("Download complete.", bytes_received=len(raw_bytes))

        return raw_bytes

    except requests.RequestException as e:
        log.error(
            f"HTTP request error during download process for URL: {url} with error: {e}"
        )
        raise


# discovery

# src/discovery/discovery_run.py (Continuación del código anterior)

import duckdb
import io

# Importamos la función de tu archivo extract.py
from src.extract.hn_extractor import extract_hn_data_bytes

# --- Configuración ---
HN_DATA_URL = "URL_AL_CSV_GZ_DE_HACKERNEWS"
# ... (otras definiciones como HN_RAW_SCHEMA)


def run_in_memory_discovery(url: str):
    """
    Ejecuta el descubrimiento de datos leyendo los bytes extraídos
    directamente de la memoria RAM con DuckDB.
    """
    # 1. Ejecutar la extracción (Carga los 4.6GB a RAM)
    raw_bytes = extract_hn_data_bytes(url)

    # 2. Convertir los bytes en un objeto tipo "archivo en memoria"
    # Este objeto 'buffer' actúa como un archivo temporal en RAM.
    # DuckDB puede leer directamente de estos buffers usando la extensión 'httpfs' o 's3'
    # si se pasa como un camino virtual, pero el método más simple es usar Pandas/Parquet
    # o la extensión 'duckdb.fs' si el archivo no fuera comprimido.

    # PERO, para archivos .csv.gz **en memoria**, la manera más fiable de que DuckDB
    # sepa que es un .gz es **escribirlo temporalmente al disco** (la forma más fácil)
    # o usar un "mount" virtual de DuckDB.

    # Opción A (Escribir a Disco Temporal, más seguro para .gz y 4.6GB)
    # Si la RAM es un cuello de botella, esto es necesario.
    temp_path = "temp_hn_data.csv.gz"
    with open(temp_path, "wb") as f:
        f.write(raw_bytes)

    # 3. DuckDB lee el archivo temporal (Aquí es donde ocurre el Schema-on-Read)
    with duckdb.connect() as conn:
        print("DuckDB leyendo el archivo temporal...")

        # DuckDB puede leer desde archivos GZ directamente desde disco:
        df_discovery = conn.execute(f"""
            SELECT * FROM read_csv_auto('{temp_path}', compression='gzip', header=True);
        """).fetchdf()

        # Ejecutar tus queries de descubrimiento sobre la tabla (ej. SELECT COUNT(*), etc.)
        total_rows = conn.execute(
            f"SELECT COUNT(*) FROM read_csv_auto('{temp_path}', compression='gzip')"
        ).fetchone()[0]
        print(f"Total de filas descubiertas: {total_rows}")

    # 4. Limpieza
    import os

    os.remove(temp_path)

    # 5. Cargar el resultado de discovery (ej. el esquema inferido) a S3 ... (Siguiente paso)
