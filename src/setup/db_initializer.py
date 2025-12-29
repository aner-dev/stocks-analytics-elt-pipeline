# src/setup/db_initializer.py

from importlib import resources

import structlog
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

log = structlog.get_logger()

DDL_RESOURCE_PACKAGE = "src.sql.ddl"


def execute_ddl_setup(conn_id: str):
    """
    Ejecuta scripts DDL de forma atómica usando recursos del paquete.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    log.info(f"🚀 Starting DDL execution for Connection ID: {conn_id}")

    try:
        # 1. Obtener archivos del paquete de recursos
        package_root = resources.files(DDL_RESOURCE_PACKAGE)

        # Filtrar y ordenar archivos SQL
        ddl_files = sorted(
            [f for f in package_root.iterdir() if f.name.endswith(".sql")],
            key=lambda f: f.name,
        )

        if not ddl_files:
            log.warning(f"⚠️ No SQL files found in {DDL_RESOURCE_PACKAGE}")
            return

        # 2. Ejecución Atómica (Una sola transacción para todo)
        with engine.begin() as connection:
            # Asegurar esquema antes de nada
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS stocks;"))

            for ddl_file in ddl_files:
                filename = ddl_file.name
                log.info(f"📜 Executing DDL: {filename}")

                sql_content = ddl_file.read_text()

                if sql_content.strip():
                    connection.execute(text(sql_content))
                else:
                    log.debug(f"Skipping empty file: {filename}")

        log.info("✅ All DDL scripts executed successfully.")

    except Exception as e:
        log.error(f"❌ Fatal Error executing DDL scripts: {e}")
        raise
