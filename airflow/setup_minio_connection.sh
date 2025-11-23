#!/bin/bash

# Script to set up Airflow MinIO connection using .env variables
# Run this script from the project root directory

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if .env file exists
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "Error: .env file not found at $SCRIPT_DIR/.env"
    echo "Please create a .env file with your MinIO credentials"
    exit 1
fi

# Source the .env file
source "$SCRIPT_DIR/.env"

# Check if Airflow is running
if ! docker compose -f docker/docker-compose.airflow.yml ps | grep -q "airflow-scheduler"; then
    echo "Error: Airflow is not running. Start it first: docker compose -f docker/docker-compose.airflow.yml up -d"
    exit 1
fi

echo "Setting up MinIO connection: minio_conn"

# Delete existing connection if it exists
docker compose -f docker/docker-compose.airflow.yml exec -T airflow-scheduler airflow connections delete minio_conn 2>/dev/null || true

# Add new MinIO connection
# MinIO uses S3-compatible API, so we use 'aws' connection type
docker compose -f docker/docker-compose.airflow.yml exec -T airflow-scheduler airflow connections add 'minio_conn' \
    --conn-type 'aws' \
    --conn-login "$MINIO_ACCESS_KEY" \
    --conn-password "$MINIO_SECRET_KEY" \
    --conn-extra "{\"endpoint_url\": \"http://$MINIO_ENDPOINT\", \"aws_access_key_id\": \"$MINIO_ACCESS_KEY\", \"aws_secret_access_key\": \"$MINIO_SECRET_KEY\", \"region_name\": \"us-east-1\"}"

echo "MinIO connection setup complete"