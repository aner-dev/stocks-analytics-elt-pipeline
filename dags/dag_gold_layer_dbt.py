# from __future__ import annotations

import pendulum

# 1. Essential Cosmos Components
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import TestBehavior, ExecutionMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

# 2. dbt Configuration - ProjectConfig
# Tells Cosmos where the dbt project root folder is located.
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/stocks_dbt_project/"

# 3. dbt Configuration - ProfileConfig
# Uses the Airflow connection to authenticate dbt against the DB (PostgreSQL).
# The key (profile_args) must match your profile name in dbt/profiles.yml (if you were to use it, but Cosmos generates it).
# In this case, we use Cosmos' profile mapping.
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="stocks_dwh_dbt_profile",  # Internal name that Cosmos will use.
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        # The Airflow key to use for the dbt connection.
        conn_id="postgres_stocks_dwh",
        # Target schema for dbt.
        profile_args={"schema": "analytics"},
    ),
)

# 4. Execution Configuration - ExecutionConfig
# Tells Cosmos how to execute dbt-core.
# Crucial: Point to the virtualenv created in the Dockerfile.
DBT_EXECUTION_CONFIG = ExecutionConfig(
    # Executes dbt via the CLI inside the virtualenv.
    execution_mode=ExecutionMode.VIRTUALENV,
    # Path to the virtualenv where you installed dbt-postgres.
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)


# 5. DbtDag Definition (The Main DAG)
# The convention is to use a single DbtDag per dbt project.
@DbtDag(
    dag_id="gold_layer_dbt_dag",
    schedule=None,  # Runs manually or after the Silver DAG.
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "gold", "stocks"],
    # Critical Cosmos Configurations
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=DBT_PROFILE_CONFIG,
    execution_config=DBT_EXECUTION_CONFIG,
    # dbt Test Behavior
    test_behavior=TestBehavior.AFTER_EACH,  # Runs tests after each model
    # test_behavior=TestBehavior.AFTER_ALL, # Runs tests after all models
)
def gold_layer_dbt_dag():
    # 6. dbt Task Execution
    # Cosmos automatically translates dbt models into Airflow tasks.
    # dbt_run_task and dbt_test_task are added automatically.
    # Here you can add specific tasks or use the default ones:

    # 1. Task to run all models (executes 'dbt run')
    from cosmos import DbtTaskGroup

    # Optional: Define a task group if you want to control the order.
    dbt_tasks = DbtTaskGroup(
        group_id="dbt_run_models",
        default_args={"retries": 1},
        # Select what to run from your dbt project.
        # Default is 'dbt run' for all models.
        # You can use select="tag:daily" for subsets.
        # In this case, it will run everything: Staging -> Marts.
    )

    # This task will run dbt run and dbt test by default (due to the config above).
    # dbt_tasks


# DAG Instance (required for Airflow)
gold_dbt_dag = gold_layer_dbt_dag()
