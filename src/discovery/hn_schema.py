# src/discovery/hn_discovery.py

import duckdb
import io
import json
import polars as pl
import structlog

# Assuming 'extract_hn_data_bytes' is importable from your project structure
from src.extract.hn_extract import extract_hn_data_bytes

log = structlog.get_logger()
HN_DATA_URL = "URL_TO_HACKERNEWS_CSV_GZ"  # Use the real constant here


def run_in_memory_discovery(url: str):
    """
    Executes data discovery by reading the extracted raw bytes directly
    from RAM using Polars for decompression and DuckDB for SQL analysis.
    This avoids temporary disk I/O for compressed files.
    """
    log.info("Starting in-memory Data Discovery", source_url=url)

    # 1. EXECUTE EXTRACTION (Network I/O: loads 4.6GB to RAM)
    # The bytes are now in the Python process memory.
    raw_bytes = extract_hn_data_bytes(url)

    # 2. DECOMPRESSION AND LOAD TO POLARS (CPU/RAM, no Disk I/O)
    # Polars is highly efficient at reading GZip content from an in-memory buffer.
    buffer = io.BytesIO(raw_bytes)

    log.info("Loading compressed bytes into Polars DataFrame...")

    # Polars automatically handles the GZ decompression from the buffer
    df_pl = pl.read_csv(
        buffer,
        infer_schema_length=100000,
        separator=",",
        try_parse_dates=False,  # Better to cast dates later in dbt
    )

    log.info("Polars Schema Inferred. Starting SQL analysis.")

    # 3. EXECUTE DISCOVERY QUERIES WITH DUCKDB (SQL over in-memory DF)
    with duckdb.connect() as conn:
        # Register the Polars DataFrame as a DuckDB view.
        # key step: a zero-copy operation in RAM.
        conn.register("raw_hn", df_pl)

        # 3a. Get the Inferred Schema (Crucial for writing dbt Staging)
        schema_query = "DESCRIBE raw_hn"
        schema_df = conn.execute(schema_query).fetchdf()

        # 3b. Convertir el resultado a un formato útil (diccionario Python)
        inferred_schema = {
            row["column_name"]: row["column_type"]
            for index, row in schema_df.iterrows()
        }

        print("\n--- INFERRED SCHEMA (Output for hn_schema.py) ---")
        print(json.dumps(inferred_schema, indent=2))

        # 3c. execute data quality queries (e.g. file counting)
        total_rows = conn.execute("SELECT COUNT(*) FROM raw_hn").fetchone()[0]
        log.info(f"Total rows discovered: {total_rows}")

    log.info("Discovery analysis complete.")

    return inferred_schema

    # The Polars and raw_bytes objects are naturally garbage collected, freeing the RAM.


if __name__ == "__main__":
    run_in_memory_discovery(HN_DATA_URL)
