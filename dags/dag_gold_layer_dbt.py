import os
from pathlib import Path

import pendulum
from airflow.models.param import Param
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode, TestBehavior

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
DBT_PROJECT_PATH = Path(f"{AIRFLOW_HOME}/dags/dbt/elt_pipeline_stocks")
PROFILES_YML_PATH = DBT_PROJECT_PATH / "profiles.yml"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

gold_layer_dbt_dag = DbtDag(
    dag_id="gold_layer_dbt_dag",
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description="If is True, execute'dbt run --full-refresh' recreating all the tables.",
        )
    },
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
    ),
    profile_config=ProfileConfig(
        profile_name="elt_pipeline_stocks",
        target_name="dev",
        profiles_yml_filepath=PROFILES_YML_PATH,
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_deps=False,
        select=["path:models/gold", "path:seeds"],
        test_behavior=TestBehavior.AFTER_ALL,
    ),
    operator_args={
        "install_deps": False,
        "full_refresh": "{{ params.full_refresh }}",  # Jinja Template that read the value from UI
    },
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "gold", "stocks"],
)
