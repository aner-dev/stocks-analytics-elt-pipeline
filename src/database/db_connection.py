from contextlib import contextmanager
import logging
import os
from typing import Dict, Tuple
import psycopg2
from psycopg2.extensions import connection, cursor

log.= logging.getLogger(__name__)


def get_db_config() -> Dict[str, str]:
    required_vars = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_SSLMODE": os.getenv("DB_SSLMODE")
    }

    missing = [var for var, value in required_vars.items() if not value]

    if missing:
        log.critical("Missing database configuration: %s", missing)
        raise ValueError(f"missing required environment variables: {missing}")

    return required_vars

def connecting_db() -> Tuple[connection, cursor]:
    try:
        config = get_db_config()
        conn = psycopg2.connect(
            host=config["DB_HOST"],
            database=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASSWORD"],
            port=config["DB_PORT"],
            connect_timeout=10,
            sslmode=["DB_SSLMODE"]
        )

        cur = conn.cursor()
        log.info("database connection established to %s", config["DB_HOST"])

        return conn, cur

    except psycopg2.OperationalError as e:
        log.exception(f"CRITICAL error: credentials not founded {e}")
        raise


if __name__ == "__main__":

@contextmanager
def connect_db_context() -> connection:
    conn = None
    try:
        config = get_db_config()

        conn = psycopg2.connect(
            host=config["DB_HOST"],
            database=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASSWORD"],
            port=config["DB_PORT"],
            connect_timeout=10,
        )

        log.info("database connection established to %s", config["DB_HOST"])

        yield conn

        conn.commit()

    except psycopg2.OperationalError as e:
        log.exception(f"CRITICAL error: failed to connect db or credentials invalid. {e}") 

        raise

    except Exception as e:

        if conn:
            conn.rollback()
        log.error(f"transaction failed, changes rolled back. Error: {e}", exc_info=True)
        raise

    finally:
        if conn:
            conn.close()
            log.info("Database connection closed.")

if __name__ == "__main__":


