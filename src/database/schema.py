from loguru import logger
import psycopg2
from dotenv import load_dotenv
import os

# Cargar variables de entorno PostgreSQL
env_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "postgres.env")
load_dotenv(env_path)


def create_sales_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS sales_data (
        -- Tu código aquí: define columnas, tipos, constraints
        -- Basado en: year:BIGINT, month:BIGINT, supplier:VARCHAR, etc.
    );
    """

    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "sales_db"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
        )

        with conn.cursor() as cur:
            cur.execute(ddl)
            conn.commit()

        logger.info("✅ Tabla creada exitosamente")
        return True

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    create_sales_table()
