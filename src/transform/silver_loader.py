# src/load/silver_loader.py

from datetime import datetime, timezone

import structlog

from src.config.settings import S3_BUCKET
from src.load.db_connector import (
    load_dataframe_to_db,
)  # write to DB

# 🚨 I/O Imports: SOLO conexiones y drivers
from src.load.s3_load import read_bronze_by_key  # Lectura de S3

# --------------------------------------------------------------------------------------
# 🚨 Transformación Pura (T): Lógica de negocio
from src.transform.polars_transform import standardize_and_clean_df

# --------------------------------------------------------------------------------------

log = structlog.get_logger()


def transform_and_load_silver(key: str, conn_id: str) -> dict:
    """
    Orchestrates the T & L process for a single stock symbol.
    Handles I/O (S3 read, DB connection, DB write) and calls pure transformation logic.
    """

    ingestion_timestamp = datetime.now(timezone.utc)
    try:
        # CHANGE: derive symbol from key instead of assuming "latest per symbol"
        symbol = key.split("/")[2]

        log.info(
            "Starting Silver load",
            bucket=S3_BUCKET,
            key=key,
            symbol=symbol,
            conn_id=conn_id,
        )

        # CHANGE: explicit Bronze read
        raw_data_json = read_bronze_by_key(
            object_name=key,
        )

        # Transform (PURE)
        df_clean, initial_rows = standardize_and_clean_df(
            raw_data_json=raw_data_json,
            symbol=symbol,
            ingestion_timestamp=ingestion_timestamp,
        )

        final_rows = df_clean.shape[0]

        if initial_rows == 0:
            log.warning("No rows found in Bronze", symbol=symbol)
            return {"symbol": symbol, "rows_processed": 0, "rows_loaded": 0}

        if final_rows == 0:
            log.warning("All rows removed during transform", symbol=symbol)
            return {"symbol": symbol, "rows_processed": initial_rows, "rows_loaded": 0}

        loaded_rows = load_dataframe_to_db(
            df_clean=df_clean,
            symbol=symbol,
            conn_id=conn_id,
        )

        return {
            "symbol": symbol,
            "rows_processed": initial_rows,
            "rows_loaded": loaded_rows,
        }

    except Exception as e:
        log.error(
            "Fatal error in Silver T/L",
            bucket=S3_BUCKET,
            key=key,
            error=str(e),
            exc_info=True,
        )
        raise
