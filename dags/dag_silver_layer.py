from airflow.decorators import dag, task
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


# --- DBT / COSMOS ---
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

log = structlog.get_logger()

# --- CONFIGURATION CONSTANTS ---
POSTGRES_CONN_ID = "postgres_stocks_dwh"
DBT_PROJECT_NAME = "elt_pipeline_stocks"
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/elt_pipeline_stocks"
DBT_EXECUTABLE_PATH = "dbt"


@dag(
    dag_id="alpha_vantage_silver_layer",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
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

    @task
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

    # --- Extraction ---
    extracted_data = extract_raw_data.expand(
        symbol=symbols_payload.map(lambda x: x["symbol"]),
        base_url=symbols_payload.map(lambda x: x["base_url"]),
        api_params=symbols_payload.map(lambda x: x["api_params"]),
    )
    symbols_payload >> extracted_data

    # --- Validation ---
    validated_data = validate_raw_payload_task.expand(extracted_data=extracted_data)

    # --- Bronze Load ---
    loaded_bronze = load_to_bronze.expand(extracted_data=validated_data)

    # --- Silver Load ---
    loaded_silver = transform_and_load_silver_task.expand(
        key=loaded_bronze.map(lambda x: x["key"]),
        conn_id=[POSTGRES_CONN_ID],
    )

    # ------------------------------------------------------------
    # PHASE 3 — GOLD (DBT)
    # ------------------------------------------------------------

    profile_config = ProfileConfig(
        profile_name=DBT_PROJECT_NAME,
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id=POSTGRES_CONN_ID,
            profile_args={"schema": "marts"},
        ),
    )

    gold_layer_dbt = DbtTaskGroup(
        group_id="dbt_gold_layer_modeling",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        default_args={
            "task_args": {"dbt_cli_flags": ["--target", "dev", "--models", "tag:gold"]}
        },
    )

    loaded_silver >> gold_layer_dbt


alpha_vantage_silver_layer()
