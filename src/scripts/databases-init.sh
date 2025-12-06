#!/bin/bash
set -e
# La corrección del loop (99% CPU) se asegura de que la DB de 'airflow' exista antes
# de que el proceso 'airflow-init' intente la migración (airflow db init).
# El problema de CPU es casi siempre una falla en la inicialización/acceso a la DB.

if [[ -z "$ELT_PASSWORD" ]]; then
  echo "ERROR: ELT_PASSWORD environment variable is required"
  exit 1
fi

# El 'until pg_isready' ya lo gestiona Docker, pero mantenerlo es seguro.
until pg_isready -U "$POSTGRES_USER" -d postgres; do
  sleep 2
done

echo "PostgreSQL is ready, starting database setup..."

# -------------------------------------------------------------
# NOTA: Usamos comandos CREATE USER/DATABASE sencillos.
# El script se ejecuta en /docker-entrypoint-initdb.d/, lo que
# significa que SOLO se ejecuta en la PRIMERA EJECUCIÓN del contenedor.
# Por lo tanto, no necesitamos los bloques DO $$ IF NOT EXISTS $$.
# -------------------------------------------------------------

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    
    -- 1. Create Users
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE USER aner WITH PASSWORD '${ELT_PASSWORD}';

    -- 2. Create Databases and set ownership
    CREATE DATABASE airflow OWNER airflow;
    CREATE DATABASE hn_db OWNER aner;

    -- 3. Grant Permissions
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE hn_db TO aner;

EOSQL

echo "Database initialization completed successfully"
# El servicio 'airflow-init' ahora puede hacer 'airflow db init'
