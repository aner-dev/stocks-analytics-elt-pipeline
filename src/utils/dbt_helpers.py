# src/utils/dbt_helpers.py
from cosmos.config import ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Airflow connection name (created in init_airflow.sh)
DBT_CONN_ID = "postgres_stocks_dwh"
DBT_PROJECT_NAME = "stocks_dbt_project"
DBT_SCHEMA = "stocks_dwh"  # The schema you created in your DDL


def get_dbt_profile_config() -> ProfileConfig:
    """Returns the dbt profile configuration using the Cosmos Profile Mapping."""
    postgres_profile_mapping = PostgresUserPasswordProfileMapping(
        conn_id=DBT_CONN_ID, profile_args={"port": 5432, "schema": DBT_SCHEMA}
    )

    return ProfileConfig(
        profile_name=DBT_PROJECT_NAME,
        target_name="dev",  # Or the target you are using in your dbt_project.yml
        profile_mapping=postgres_profile_mapping,
        # If you had the original profiles.yml:
        # profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml"
        # But with the mapping, this is no longer necessary
    )

