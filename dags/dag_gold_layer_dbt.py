# dag_gold_layer_dbt.py
import os
import pendulum
from airflow.sdk.definitions.asset import Asset as Dataset
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior, ExecutionMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")

DBT_PROJECT_PATH = f"{AIRFLOW_HOME}/dags/dbt/elt_pipeline_stocks"

STOCKS_SILVER_DATASET = Dataset(
    "postgres://localhost/stocks_dwh/stocks/stg_weekly_adjusted_prices"
)
profile_config = ProfileConfig(
    profile_name="stocks_dwh_dbt_profile",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_stocks_dwh",
        profile_args={"dbname": "stocks_dwh", "schema": "silver"},
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    dbt_executable_path=f"{AIRFLOW_HOME}/dbt_venv/bin/dbt",
)


gold_layer_dbt_dag = DbtDag(
    dag_id="gold_layer_dbt_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[STOCKS_SILVER_DATASET],  # triggered after silver
    catchup=False,
    tags=["dbt", "gold", "stocks"],
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        partial_parse=False,
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_EACH,
    ),
    operator_args={
        "select": ["tag:gold"],
        "install_deps": True,
        "py_requirements": ["dbt-postgres==1.8.2"],
        "env": {
            "DBT_PACKAGES_DIR": "dbt_packages",
        },
    },
)
