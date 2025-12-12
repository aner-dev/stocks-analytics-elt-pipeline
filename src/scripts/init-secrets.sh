#!/bin/bash
# --- LocalStack Initialization Script (Secrets Manager) ---
#
# PURPOSE: Ensures the 'airflow/variables/...' and other aws secrets are created upon startup.
# NECESSITY: The secret list was lost on every 'astro dev restart', failing the
# Airflow Secrets Backend integration test. This script guarantees the
# required initial state before Airflow attempts to read the variable.
# -----------------------------------------------------------

# 1. Define the Region and Secret Name
REGION="us-east-1"
SECRET_ID="airflow/variables/alpha_vantage_api_key"
SECRET_VALUE="NW35VP51I8YI9KYV"

# 2. Wait for SecretsManager to be ready (optional, but safe)
echo "Waiting for Secrets Manager to be available..."
/usr/bin/awslocal secretsmanager list-secrets --region $REGION

# 3. Create the Secret
echo "Creating the secret: $SECRET_ID in $REGION..."
/usr/bin/awslocal secretsmanager create-secret \
  --name $SECRET_ID \
  --secret-string "$SECRET_VALUE" \
  --region $REGION
echo "Secret created."

# 4. (Optional) List to confirm
echo "Confirming secret creation..."
/usr/bin/awslocal secretsmanager list-secrets --region $REGION | grep $SECRET_ID
