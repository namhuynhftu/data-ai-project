#!/bin/bash

# Script to set up Airflow Snowflake connection using .env variables
# Run this script from the lab directory

set -e

# Check if .env file exists
if [ ! -f "./airflow/.env" ]; then
    echo "Error: .env file not found in current directory"
    echo "Please create a .env file with your Snowflake credentials"
    exit 1
fi

# Source the .env file
source ./airflow/.env

# Check if Airflow is running
if ! docker ps --format '{{.Names}}' | grep -q "airflow_scheduler"; then
    echo "Error: Airflow is not running. Start it first: docker compose -f docker/docker-compose.airflow.yml up -d"
    exit 1
fi

# Read and encode private key content (base64 encoding for safe transmission)
if [ ! -f "$SNOWFLAKE_PRIVATE_KEY_FILE_PATH" ]; then
    echo "Error: Private key file not found at $SNOWFLAKE_PRIVATE_KEY_FILE_PATH"
    exit 1
fi

# Base64 encode the private key (compatible with both macOS and Linux)
PRIVATE_KEY_CONTENT=$(cat "$SNOWFLAKE_PRIVATE_KEY_FILE_PATH" | base64 | tr -d '\n')

echo "Setting up Snowflake connection: snowflake_conn"

# Delete existing connection if it exists
docker exec airflow_scheduler airflow connections delete snowflake_conn 2>/dev/null || true

# Add new connection
docker exec airflow_scheduler airflow connections add 'snowflake_conn' \
    --conn-type 'snowflake' \
    --conn-login "$SNOWFLAKE_USER" \
    --conn-password "$SNOWFLAKE_PRIVATE_KEY_FILE_PWD" \
    --conn-schema "$SNOWFLAKE_SCHEMA" \
    --conn-extra "{\"account\": \"$SNOWFLAKE_ACCOUNT\", \"warehouse\": \"$SNOWFLAKE_WAREHOUSE\", \"database\": \"$SNOWFLAKE_DATABASE\", \"role\": \"$SNOWFLAKE_ROLE\", \"private_key_content\": \"$PRIVATE_KEY_CONTENT\"}"

echo "Snowflake connection setup complete"source 