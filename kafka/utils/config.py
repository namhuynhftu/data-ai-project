"""
Centralized configuration management for the Kafka streaming lab.

This module provides consistent access to environment variables and default
configuration values across all components of the application.
"""

import os

from dotenv import load_dotenv


path ="../../config/app/development.env"
load_dotenv(path)

from utils.models import KafkaConfig, PostgresConfig


def get_postgres_config() -> PostgresConfig:
    """
    Get PostgreSQL configuration from environment variables.

    Returns:
        PostgresConfig object containing PostgreSQL connection parameters with
        consistent defaults.
    """
    return PostgresConfig(
        user=os.getenv("POSTGRES_KAFKA_USER"),
        password=os.getenv("POSTGRES_KAFKA_PASSWORD"),
        host=os.getenv("POSTGRES_KAFKA_HOST"),
        port=os.getenv("POSTGRES_KAFKA_PORT"),
        database=os.getenv("POSTGRES_KAFKA_DB"),
        schema=os.getenv("POSTGRES_KAFKA_SCHEMA")
    )


def get_kafka_config() -> KafkaConfig:
    """
    Get Kafka configuration from environment variables.

    Returns:
        KafkaConfig object containing Kafka connection parameters with
        consistent defaults.
    """
    return KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        topic=os.getenv("KAFKA_TOPIC", "streaming_invoices"),
    )