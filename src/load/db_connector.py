# db_connector.py
from typing import Any, Dict

import polars as pl
import structlog

# 🚨 Key Airflow Import
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import extras

log = structlog.get_logger()

# --- CONFIGURATION (CONSTANTS) ---
TABLE_NAME = "weekly_adjusted_prices"
SCHEMA_NAME = "stocks"
AUDIT_TABLE = f"{SCHEMA_NAME}.pipeline_audit"  # Using the schema constant


def load_dataframe_to_db(df_clean: pl.DataFrame, symbol: str, conn_id: str) -> int:
    """
    Loads the Polars DataFrame directly into PostgreSQL using psycopg2.extras.execute_values
    via the Airflow PostgresHook connection.
    """
    hook = None
    conn = None
    rows_loaded = 0

    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        conn.autocommit = False  # type: ignore
        cur = conn.cursor()

        log.info(f"🔄 Starting bulk load for symbol: {symbol}. Rows: {df_clean.height}")

        # 1. Polars to List of Tuples
        data_to_insert = df_clean.rows()

        # 2. Build the query
        columns = df_clean.columns
        columns_str = ", ".join(columns)

        table_full_name = f"{SCHEMA_NAME}.{TABLE_NAME}"
        insert_query = f"INSERT INTO {table_full_name} ({columns_str}) VALUES %s ON CONFLICT DO NOTHING"

        # 3. Bulk Insert
        extras.execute_values(
            cur,
            insert_query,
            data_to_insert,
            page_size=10000,
        )

        conn.commit()
        rows_loaded = len(data_to_insert)
        log.info(f"✅ Load successful for symbol {symbol}. Rows written: {rows_loaded}")
        return rows_loaded

    except Exception as e:
        log.error(
            f"❌ Failed to load data for symbol {symbol} to DB: {e}", exc_info=True
        )
        if conn:
            conn.rollback()
        raise RuntimeError(f"DB Load failed for {symbol}. Error: {e}")

    finally:
        if conn:
            conn.close()


def insert_audit_metrics(metrics: Dict[str, Any], conn_id: str) -> None:
    """
    Inserts pipeline observability metrics into the Audit table using the Airflow Hook.
    """
    hook = None
    conn = None

    sql_insert = f"""
    INSERT INTO {AUDIT_TABLE} (
        pipeline_step, stock_symbol, rows_processed, rows_inserted, rows_rejected, 
        duration_seconds, run_timestamp
    )
    VALUES (
        'transform_load_silver', %s, %s, %s, %s, %s, NOW()
    );
    """

    rows_processed = metrics.get("rows_processed", 0)
    rows_loaded = metrics.get("rows_loaded", 0)

    params_tuple = (
        metrics.get("symbol", "N/A"),
        rows_processed,
        rows_loaded,
        rows_processed - rows_loaded,
        metrics.get("duration_seconds", 0.0),
    )

    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        conn.autocommit = False  # type: ignore
        cur = conn.cursor()

        cur.execute(sql_insert, params_tuple)
        conn.commit()

        log.info(f"✅ Audit successfully inserted for {params_tuple[0]}.")

    except Exception as e:
        log.error(f"❌ Warning: Failed to insert audit metrics: {e}")
        if conn:
            conn.rollback()  # type: ignore

    finally:
        if conn:
            conn.close()


# 🚨 Remove or comment out any test or helper function that depends on Secrets Manager here.
