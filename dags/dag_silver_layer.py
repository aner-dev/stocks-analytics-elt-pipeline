# dag_silver_layer.py
# FIXME: merge issue in PR/pull request in github (pyproject.toml & uv.lock)
# TODO: create dag_gold_layer.py and finish dbt/data modeling phase

from airflow.decorators import dag, task
from airflow.sdk.definitions.asset import Asset as Dataset
from datetime import datetime, timedelta
import structlog

# --- BUSINESS LOGIC IMPORTS ---
from src.utils.dag_helpers import define_symbols_and_parameters
from src.setup.db_initializer import execute_ddl_setup
from src.extract.extract_stocks import extract_stocks_data
from src.data_quality.validator import validate_raw_payload
from src.load.s3_load import write_bronze
from src.aws.boto_client import ensure_bucket_exists
from src.transform.silver_loader import transform_and_load_silver
from src.config.settings import S3_BUCKET

log = structlog.get_logger()

# --- CONFIGURATION CONSTANTS ---
POSTGRES_CONN_ID = "postgres_stocks_dwh"
DBT_PROJECT_NAME = "elt_pipeline_stocks"
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/elt_pipeline_stocks"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
STOCKS_SILVER_DATASET = Dataset(
    "postgres://stocks_dwh_postgres:5432/stocks_dwh/stocks/weekly_adjusted_prices"
)


@dag(
    dag_id="alpha_vantage_silver_layer",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "alpha_vantage", "finance", "stocks", "mapped", "dbt"],
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(hours=1),
    },
)
def alpha_vantage_silver_layer():
    # ------------------------------------------------------------
    # PHASE 1 — SETUP / CONFIGURATION
    # ------------------------------------------------------------
    @task
    def setup_s3_bucket(bucket: str) -> None:
        ensure_bucket_exists(bucket)

    @task
    def setup_database_tables(conn_id: str) -> None:
        execute_ddl_setup(conn_id=conn_id)

    @task
    def generate_symbols_payload() -> list[dict]:
        return define_symbols_and_parameters()

    db_setup = setup_database_tables(conn_id=POSTGRES_CONN_ID)
    s3_setup = setup_s3_bucket(bucket=S3_BUCKET)
    symbols_payload = generate_symbols_payload()

    db_setup >> s3_setup >> symbols_payload

    # ------------------------------------------------------------
    # PHASE 2 — ELT (MAPPED PER SYMBOL)
    # ------------------------------------------------------------

    @task(pool="alpha_vantage_api", retries=3, retry_delay=timedelta(seconds=70))
    def extract_raw_data(
        base_url: str,
        api_params: dict,
        symbol: str,
    ) -> dict:
        # CHANGE: explicit task signature to avoid Airflow context injection (**kwargs)
        # and keep business logic decoupled from Airflow
        return extract_stocks_data(
            base_url=base_url,
            api_params=api_params,
            symbol=symbol,
        )

    @task
    def validate_raw_payload_task(extracted_data: dict) -> dict:
        return validate_raw_payload(extracted_data)

    @task
    def load_to_bronze(extracted_data: dict) -> dict:
        return write_bronze(
            json_data=extracted_data["raw_json"],
            symbol=extracted_data["symbol"],
        )

    @task
    def transform_and_load_silver_task(key: str, conn_id: str) -> None:
        transform_and_load_silver(key=key, conn_id=conn_id)

    @task(outlets=[STOCKS_SILVER_DATASET])
    def notify_silver_completed(upstream_results):
        return "Ready for Gold!"

    # --- Extraction ---
    extracted_data = extract_raw_data.expand(
        symbol=symbols_payload.map(lambda x: x["symbol"]),
        base_url=symbols_payload.map(lambda x: x["base_url"]),
        api_params=symbols_payload.map(lambda x: x["api_params"]),
    )
    [db_setup, s3_setup] >> extracted_data

    # --- Validation ---
    validated_data = validate_raw_payload_task.expand(extracted_data=extracted_data)

    # --- Bronze Load ---
    loaded_bronze = load_to_bronze.expand(extracted_data=validated_data)

    # --- Silver Load ---
    silver_results = transform_and_load_silver_task.expand(
        key=loaded_bronze.map(lambda x: x["key"]),
        conn_id=[POSTGRES_CONN_ID],
    )
    notify_silver_completed(silver_results)


alpha_vantage_silver_layer()
