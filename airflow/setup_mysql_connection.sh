#!/bin/bash

# Script to set up Airflow MySQL connection using .env variables
# Run this script from the lab directory

set -e

# Check if .env file exists
if [ ! -f "./airflow/.env" ]; then
    echo "Error: .env file not found in current directory"
    echo "Please create a .env file with your MySQL credentials"
    exit 1
fi

# Source the .env file
source ./airflow/.env

# Check if Airflow is running
if ! docker compose -f docker/docker-compose.airflow.yml ps | grep -q "airflow-scheduler"; then
    echo "Error: Airflow is not running. Start it first: docker compose -f docker/docker-compose.airflow.yml up -d"
    exit 1
fi

echo "Setting up MySQL connection: mysql_conn"

# Delete existing connection if it exists
docker compose -f docker/docker-compose.airflow.yml exec -T airflow-scheduler airflow connections delete mysql_conn 2>/dev/null || true

# Add new connection with SSL disabled (for local development)
docker compose -f docker/docker-compose.airflow.yml exec -T airflow-scheduler airflow connections add 'mysql_conn' \
    --conn-type 'mysql' \
    --conn-host "$MYSQL_HOST" \
    --conn-port "$MYSQL_PORT" \
    --conn-login "$MYSQL_USER" \
    --conn-password "$MYSQL_PASSWORD" \
    --conn-schema "$MYSQL_DATABASE" \
    --conn-extra '{"ssl_disabled": "True"}'

echo "MySQL connection setup complete"