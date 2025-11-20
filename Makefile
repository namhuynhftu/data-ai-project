# ==============================================================================
# Makefile for Data Engineering Pipeline
# ==============================================================================
# Usage:
#   make help           - Show all available commands
#   make up             - Start all services (batch + streaming)
#   make down           - Stop all services
#   make restart        - Restart all services
#   make logs           - View logs from all services
#   make status         - Show status of all containers
# ==============================================================================

.PHONY: help up down restart logs status clean build

# Default target
.DEFAULT_GOAL := help

# Docker Compose files
BATCH_COMPOSE := docker/docker-compose.batch.yml
STREAMING_COMPOSE := docker/docker-compose.streaming.yml
AIRFLOW_COMPOSE := docker/docker-compose.airflow.yml

# Project names
BATCH_PROJECT := batch
STREAMING_PROJECT := streaming
AIRFLOW_PROJECT := airflow
# ==============================================================================
# Help
# ==============================================================================
help: ## Show this help message
	@echo "========================================"
	@echo "Data Engineering Pipeline - Makefile"
	@echo "========================================"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
	@echo ""

# ==============================================================================
# Main Commands - All Services
# ==============================================================================
up: ## Start all services (batch + streaming)
	@echo "Starting all services..."
	@echo "Export the local environment variables"
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) up -d
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) up -d
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) up -d
	@echo "All services started successfully!"
	@make status

down: ## Stop all services
	@echo "Stopping all services..."
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) down
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) down
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) down
	@echo "All services stopped"

restart-begin: 
	@echo "Restarting all services..."
	@echo "Stopping all services..."
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) down
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) down
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) down
	@echo "All services stopped"
	@echo "Export the local environment variables"
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) up -d
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) up -d
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) up -d
	@echo "All services started successfully!"
	@echo "All services restarted"
	
restart-build: 
	@echo "Restarting all services..."
	@echo "Stopping all services..."
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) down -v
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) down -v
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) down -v
	@echo "All services stopped"
	@echo "Export the local environment variables"
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) up -d --build
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) up -d --build
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) up -d --build
	@echo "All services started successfully!"
	@echo "All services restarted"

build: ## Rebuild all services
	@echo "Building all services..."
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) build
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) build
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) build
	@echo "Build complete"

# ==============================================================================
# Airflow Orchestrator Commands
# ==============================================================================
airflow-up: ## Start airflow services only
	@echo "Starting Orchestration services..."
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) up -d
	@echo "Airflow services started"

airflow-down: ## Stop airflow services only
	@echo "Stopping airflow services..."
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) down
	@echo "Airflow services stopped"

airflow-logs: ## View airflow services logs
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) logs -f
airflow-build: ## Rebuild airflow services
	@docker compose -f $(AIRFLOW_COMPOSE) -p $(AIRFLOW_PROJECT) build

airflow-restart: airflow-down airflow-up ## Restart airflow services


# ==============================================================================
# Batch Pipeline Commands
# ==============================================================================
batch-up: ## Start batch services only
	@echo "Starting batch services..."
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) up -d
	@echo "Batch services started"

batch-down: ## Stop batch services only
	@echo "Stopping batch services..."
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) down
	@echo "Batch services stopped"

batch-logs: ## View batch services logs
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) logs -f

batch-build: ## Rebuild batch services
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) build

batch-restart: batch-down batch-up ## Restart batch services

# ==============================================================================
# Streaming Pipeline Commands
# ==============================================================================
streaming-up: ## Start streaming services only
	@echo "Starting streaming services..."
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) up -d
	@echo "Streaming services started"

streaming-up-build: ## Start streaming services only
	@echo "Starting streaming services..."
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) up -d --build
	@echo "Streaming services started"

streaming-down: ## Stop streaming services only
	@echo "Stopping streaming services..."
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) down
	@echo "Streaming services stopped"

streaming-restart:
	@cho "Restarting streaming services..."
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) down -v 
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) up -d --build
	@echo "Streaming services restarted"

streaming-logs: ## View streaming services logs
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) logs -f

streaming-restart: streaming-down streaming-up ## Restart streaming services

# ==============================================================================
# Monitoring & Debugging
# ==============================================================================
status: ## Show status of all containers
	@echo "========================================"
	@echo "Container Status"
	@echo "========================================"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "NAMES|mysql|minio|postgres|kafka|schema"
	@echo ""

logs: ## View logs from all services
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) logs -f &
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) logs -f

logs-mysql: ## View MySQL logs
	@docker logs mysql_data_source -f

logs-postgres: ## View PostgreSQL logs
	@docker logs postgres_streaming -f

logs-kafka: ## View Kafka logs
	@docker logs kafka_broker -f

logs-minio: ## View MinIO logs
	@docker logs data_lake_minio -f

# ==============================================================================
# Database Operations
# ==============================================================================
mysql-shell: ## Access MySQL shell
	@docker exec -it mysql_data_source mysql -u$$MYSQL_USER -p$$MYSQL_PASSWORD $$MYSQL_DATABASE

postgres-shell: ## Access PostgreSQL shell
	@docker exec -it postgres_streaming psql -U user -d streaming_db

# ==============================================================================
# Cleanup Commands
# ==============================================================================
clean: down ## Stop services and remove volumes
	@echo "Removing all volumes..."
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) down -v
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) down -v
	@echo "Cleanup complete"

clean-images: ## Remove all project images
	@echo "Removing Docker images..."
	@docker images | grep -E "batch|streaming|mysql_data_source" | awk '{print $$3}' | xargs -r docker rmi -f
	@echo "Images removed"

clean-all: clean clean-images ## Full cleanup (volumes + images)
	@echo "Complete cleanup finished"

prune: ## Remove unused Docker resources
	@echo "Pruning Docker system..."
	@docker system prune -f
	@echo "Prune complete"

# ==============================================================================
# Health Checks
# ==============================================================================
health: ## Check health of all services
	@echo "========================================"
	@echo "Service Health Status"
	@echo "========================================"
	@echo "MySQL:          $$(docker inspect mysql_data_source --format='{{.State.Health.Status}}' 2>/dev/null || echo 'not running')"
	@echo "MinIO:          $$(docker inspect data_lake_minio --format='{{.State.Health.Status}}' 2>/dev/null || echo 'not running')"
	@echo "PostgreSQL:     $$(docker inspect postgres_streaming --format='{{.State.Health.Status}}' 2>/dev/null || echo 'not running')"
	@echo "Kafka:          $$(docker inspect kafka_broker --format='{{.State.Health.Status}}' 2>/dev/null || echo 'not running')"
	@echo ""

# ==============================================================================
# Development Helpers
# ==============================================================================
dev: ## Start services in development mode (with logs)
	@docker compose -f $(BATCH_COMPOSE) -p $(BATCH_PROJECT) up &
	@docker compose -f $(STREAMING_COMPOSE) -p $(STREAMING_PROJECT) up

open-kafka-ui: ## Open Kafka UI in browser
	@echo "Opening Kafka UI..."
	@cmd /c start http://localhost:8080

open-minio: ## Open MinIO Console in browser
	@echo "Opening MinIO Console..."
	@cmd /c start http://localhost:9001

urls: ## Show all service URLs
	@echo "========================================"
	@echo "Service URLs"
	@echo "========================================"
	@echo "Kafka UI:       http://localhost:8080"
	@echo "MinIO Console:  http://localhost:9001"
	@echo "Schema Registry: http://localhost:8081"
	@echo ""
	@echo "Database Ports:"
	@echo "MySQL:          localhost:3306"
	@echo "PostgreSQL:     localhost:5432"
	@echo "Kafka:          localhost:9092"
	@echo ""

# ==============================================================================
# Testing
# ==============================================================================
test-mysql: ## Test MySQL connection
	@docker exec mysql_data_source mysql -u$$MYSQL_USER -p$$MYSQL_PASSWORD -e "SELECT 'MySQL OK' as status;"

test-postgres: ## Test PostgreSQL connection
	@docker exec postgres_streaming psql -U user -d streaming_db -c "SELECT 'PostgreSQL OK' as status;"

test-kafka: ## Test Kafka connection
	@docker exec kafka_broker /bin/bash -c "echo 'Kafka OK'"

test-all: test-mysql test-postgres test-kafka ## Test all service connections
	@echo "$(GREEN)✓ All services are responding$(NC)"
