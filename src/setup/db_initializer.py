# src/load/db_initializer.py (FORMERLY: db_setup.py)

import structlog

# Use importlib.resources to access SQL files
from importlib import resources
from typing import List
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from sqlalchemy import text

log = structlog.get_logger()

DDL_RESOURCE_PACKAGE = "src.sql.ddl"


def execute_ddl_setup(conn_id: str):
    """
    Establishes the connection using the Airflow connection ID and executes
    the DDL scripts (schema, silver, audit) from the installed package resources.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)

    log.info(
        f"Starting DDL execution for Connection ID: {conn_id}. Looking in package {DDL_RESOURCE_PACKAGE}"
    )

    try:
        # use the Hook to get the psycopg2/SQLAlchemy connection
        # SQLAlchemy is simpler for executing plain SQL than cur.execute with multiple commands.
        engine = hook.get_sqlalchemy_engine()

        package_root = resources.files(DDL_RESOURCE_PACKAGE)

        # Iterate over the files in the resource package, sorted by name (e.g., 000, 001, 003)
        # 🚨 This eliminates the need for the manual ddl_files list.
        ddl_files_iterator = sorted(
            [f for f in package_root.iterdir() if Path(str(f)).suffix == ".sql"],
            key=lambda f: f.name,
        )

        if not ddl_files_iterator:
            log.warning(
                f"No SQL files found in DDL package: {DDL_RESOURCE_PACKAGE}. Skipping setup."
            )
            return

        with engine.begin() as connection:
            for ddl_file in ddl_files_iterator:
                filename = ddl_file.name

                # 1. Read the content of the SQL file as text from the package resource
                # resources.files().joinpath().read_text() is the robust pattern
                sql_command = (
                    resources.files(DDL_RESOURCE_PACKAGE).joinpath(filename).read_text()
                )

                log.info(f"Executing DDL: {filename}")

                # 2. Execute the SQL command
                # Note: It is crucial to import 'text' from sqlalchemy if engine.execute() is used
                from sqlalchemy import text

                connection.execute(text(sql_command))

        log.info("✅ All DDL scripts executed successfully.")

    except Exception as e:
        log.error(f"❌ Fatal Error executing DDL scripts: {e}", exc_info=True)
        # Revert changes if necessary (the 'with engine.begin() as...' block handles this if there is an error)
        raise
