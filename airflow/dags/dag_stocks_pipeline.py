from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowSkipException,
)
import requests

from elt_stocks_pipeline.src.extract.extract import (
    extract_stocks_data,
    symbols_list,
)
from elt_stocks_pipeline.src.load.minio_client import (
    write_bronze,
    read_bronze,
    write_silver,
    read_parquet_by_key,
)
from elt_stocks_pipeline.src.transform.stocks_transform import dataframe_transform

# standard imports
import io
import polars as pl

import structlog

log = structlog.get_logger()


# TODO: read TODO.md (fix databases-init.sh & create airflow connection)
# --- DAG Definition ---
@dag(
    dag_id="stocks_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["main", "stocks", "elt", "api_alpha_vantage"],
)
def stocks_weekly_pipeline():
    # 1. Task to FETCH THE LIST OF SYMBOLS
    @task
    def fetch_symbols_task():
        """Task 0: Returns the list of symbols to process. The return value is passed via XComs."""
        # In a production environment, this would call a DB or a config service.
        return symbols_list

    # 2. Task for EXTRACT/LOAD (The core E->L task, executed once per symbol)
    # The 'pool' relies on Airflow for limits concurrency
    # NOTE: The 'api_rate_limit_pool' must be configured in the Airflow UI.

    @task(pool="api_rate_limit_pool", max_active_tis_per_dag=1)
    def extract_and_load_bronze_task(symbol: str):
        """Task 1: Extracts data for ONE SYMBOL, adds metadata, and loads it to the Bronze layer."""

        API_CONN_ID = "alpha_vantage_api"

        # --- Configuration (Should come from Airflow Variables/Connections, not .env) ---
        try:
            conn = BaseHook.get_connection(API_CONN_ID)

            API_KEY = conn.password

            BASE_URL = conn.host

        except AirflowException:
            log.critical(
                f"FATAL: Airflow Connection '{API_CONN_ID} not found or invalid"
            )
            raise AirflowFailException("API Connection configuration missing.")

        base_api_params = {
            "function": "TIME_SERIES_WEEKLY_ADJUSTED",
            "datatype": "json",
            "apikey": API_KEY,
        }

        symbol_params = base_api_params.copy()
        symbol_params["symbol"] = symbol

        extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # --- E->L Logic ---
        try:
            # 1. Extract
            data = extract_stocks_data(BASE_URL, symbol_params)

            if not data:
                log.error("ERROR: Extraction not complete")
                raise AirflowSkipException(f"No data returned for {symbol}")

            # 2. METADATA ENRICHMENT (Data Lineage)
            enriched_data = {
                "metadata": {
                    "extraction_timestamp": extraction_timestamp,
                    "symbol": symbol,
                    "source_system": "alpha_vantage",
                },
                "api_data": data,
            }

            # 3. Load to MinIO
            write_bronze(
                json_data=enriched_data,
                bucket="stocks-data",
                symbol=symbol,
                timestamp=extraction_timestamp,
            )
            log.info(f"Successfully loaded {symbol} to Bronze.")

            return symbol

        # --- Error Handling ---
        except AirflowSkipException as e:
            log.error(f"Task Skipped: {e}")
            raise e

        except requests.exceptions.HTTPError as e:
            # Handling API specific errors
            if e.response.status_code == 401:
                log.critical(
                    f"API Authentication failed for {symbol}.", status_code=401
                )
                raise AirflowFailException(
                    f"Auth failed (401) for {symbol}"
                )  # Fail permanently
            elif e.response.status_code == 429:
                # Rate limit. Airflow will retry based on default_args. Pool limits concurrent hits.
                log.critical(
                    f"Rate limit exceeded for {symbol}. Airflow will retry.",
                    status_code=429,
                )
                raise AirflowException(f"Rate limit exceeded (429) for {symbol}")
            else:
                log.error(
                    f"General HTTP Error for {symbol}.",
                    status_code=e.response.status_code,
                )
                raise AirflowException(f"HTTP Error for {symbol}: {e}")

        except Exception as e:
            # Catching MinIO connection errors, JSON errors, etc.
            log.critical(
                f"Unhandled exception processing {symbol}.",
                error_type=type(e).__name__,
                exc_info=True,
            )
            raise AirflowException(f"Unhandled error for {symbol}: {e}")

    @task
    def transform_single_symbol_task(symbol: str):
        """
        Task 2A: Reads ONE symbol from Bronze, transforms it using Polars (dataframe_transform method),
        and writes the temporary and individual clean Parquet file to Silver layer (minIO bucket).
        """
        try:
            # 1. READ Bronze
            raw_data = read_bronze(bucket="stocks-data", symbol=symbol)

            # 2. TRANSFORM
            df_clean = dataframe_transform(raw_data, symbol)

            # 3. WRITE TEMPORARY SILVER (Resilience step)
            buffer = io.BytesIO()
            df_clean.write_parquet(buffer, compression="snappy")
            parquet_bytes = buffer.getvalue()

            # NOTE: 'write_silver' must handle saving a single, temporary file.
            silver_key = write_silver(parquet_bytes, symbol=symbol)

            log.info(
                f"Successfully transformed and saved temporary silver file for {symbol}."
            )
            return silver_key  # Pass the file path/key for the next task

        except Exception as e:
            log.error(f"Transformation failed for {symbol}: {e}")
            # Raising an exception will fail this specific instance, but others will continue.
            raise AirflowException(f"Silver transformation failed for {symbol}: {e}")

    @task
    def consolidate_silver_files_task(silver_keys: list[str]):
        """
        Task 2B: Reads ALL temporary Silver files, concatenates them, performs audit,
        and writes the FINAL, unified Silver file.
        """
        # 1. Read all individual Parquet files from MinIO using the list of keys
        all_dataframes = []
        for key in silver_keys:
            df = read_parquet_by_key(
                bucket="stocks-data", object_name=key
            )  # Assuming a new read function
            all_dataframes.append(df)

        unified_df = pl.concat(all_dataframes)

        # 2. Write the final unified file
        buffer = io.BytesIO()
        unified_df.write_parquet(buffer, compression="snappy")
        final_key = write_silver(buffer.getvalue(), symbol=None, is_final=True)

        log.info(f"✅ Final Silver dataset consolidated and saved to: {final_key}")
        return final_key

    # --- DAG FLOW DEFINITION ---

    # 1. Get the list of symbols
    symbols_to_process = fetch_symbols_task()

    bronze_load_results = extract_and_load_bronze_task.expand(symbol=symbols_to_process)

    # 3. Silver Transformation (parrallel)
    # it expands using the list of successful symbols of the previous task
    silver_keys = transform_single_symbol_task.expand(symbol=bronze_load_results)

    # 4. Silver Consolidation (sequential)
    consolidate_silver_files_task(silver_keys)
