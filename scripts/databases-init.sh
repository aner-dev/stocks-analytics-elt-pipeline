#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
            CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
        ELSE
            ALTER USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
        END IF;
    END
    \$\$;

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'elt_user') THEN
            CREATE USER elt_user WITH PASSWORD '${ELT_USER_PASSWORD}';
        ELSE
            ALTER USER elt_user WITH PASSWORD '${ELT_USER_PASSWORD}';
        END IF;
    END
    \$\$;

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow') THEN
            CREATE DATABASE airflow OWNER airflow;
        END IF;
    END
    \$\$;

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'stocks_db') THEN
            CREATE DATABASE stocks_db OWNER elt_user;
        END IF;
    END
    \$\$;

    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE stocks_db TO elt_user;

EOSQL
