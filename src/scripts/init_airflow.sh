#!/bin/bash
# init_airflow.sh - ELT Environment Provisioner (DWH + S3)

# --- WAIT CONFIGURATION ---
DWH_HOST="stocks_dwh_postgres"
DWH_PORT="5432"
MAX_ATTEMPTS=60 # Maximum 60 seconds

# --- 1. ACTIVE WAIT FOR DWH (PostgreSQL) ---
# Usa 'nc' para esperar que el puerto 5432 esté abierto (esto sí funciona)
echo "Starting active wait for DWH service ($DWH_HOST:$DWH_PORT)..."
for i in $(seq 1 $MAX_ATTEMPTS); do
  if nc -z -w 1 $DWH_HOST $DWH_PORT; then
    echo "✅ DWH available after $i seconds."
    break
  fi
  echo "Waiting for $DWH_HOST:$DWH_PORT. Attempt $i/$MAX_ATTEMPTS..."
  sleep 1
done

if ! nc -z -w 1 $DWH_HOST $DWH_PORT; then
  echo "❌ Error: DWH service is not available after $MAX_ATTEMPTS seconds. Failing..."
  exit 1
fi

# --- 2. ESPERA FIJA PARA RUSTFS (Desbloqueo) ---
# Reemplazamos la prueba de curl inestable por un sleep fijo, que es el único método que funciona
echo "Waiting 20 seconds for RustFS/S3 to fully stabilize before provisioning..."
sleep 20
echo "✅ Assuming RustFS is now available."

# --- 3. AIRFLOW CONFIGURATION AND PROVISIONING ---
echo "✅ Services available. Proceeding with Airflow configuration."

# 3a. Airflow Variable Configuration
airflow variables set alpha_vantage_api_key 'YELVS772CHCMKLEK'
echo "Variable 'alpha_vantage_api_key' created."

# 3b. S3 Bucket Provisioning
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_ENDPOINT_URL=http://rustfs:9000

# Añadimos '|| true' para que si RustFS aún no está 100% listo, el script NO MUERA.

# 3c. Creation of Postgres DWH Connection
airflow connections delete 'postgres_stocks_dwh' || true # <-- Borra si existe, si no, ignora el error
airflow connections add 'postgres_stocks_dwh' \
  --conn-type 'postgres' \
  --conn-host 'stocks_dwh_postgres' \
  --conn-login 'postgres' \
  --conn-password 'postgres' \
  --conn-port '5432' \
  --conn-schema 'stocks_dwh' # postgres database
echo "✅ Connection 'postgres_stocks_dwh' created."

airflow connections add 'alpha_vantage_default' \
  --conn-type 'http' \
  --conn-host 'https://www.alphavantage.co' \
  --conn-password 'YELVS772CHCMKLEK'
echo "✅ Connection 'alpha_vantage_default' created."

echo "Initial Airflow configuration completed. Starting Scheduler..."
