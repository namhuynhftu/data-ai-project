from __future__ import annotations

import logging
import os

import psycopg2 as psycopg

logger = logging.getLogger(__name__)


def get_postgres_dsn() -> str:
    """Build PostgreSQL connection string from environment variables."""
    pg_user = os.getenv("POSTGRES_KAFKA_USER")
    pg_pwd = os.getenv("POSTGRES_KAFKA_PASSWORD")
    pg_host = os.getenv("POSTGRES_KAFKA_HOST")
    pg_port = os.getenv("POSTGRES_KAFKA_PORT")
    pg_db = os.getenv("POSTGRES_KAFKA_DB")
    dsn = f"postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}"
    logger.info(f"PostgreSQL connection configured for {pg_host}:{pg_port}")
    return dsn


def create_transactions_table(dsn: str) -> bool:
    """Create the transactions_sink table if it doesn't exist."""
    try:
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions_sink (
                    tx_id VARCHAR(50) PRIMARY KEY,
                    user_id VARCHAR(50),
                    amount DECIMAL(10,2),
                    currency VARCHAR(10),
                    merchant VARCHAR(100),
                    category VARCHAR(50),
                    timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        logger.info("Transactions table created/verified successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to create transactions table: {e}")
        return False


def insert_transaction(dsn: str, transaction_data: dict) -> bool:
    """Insert a transaction into the database."""
    try:
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions_sink (
                    tx_id, user_id, amount, currency, merchant, category, timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tx_id) DO NOTHING
            """,
                (
                    transaction_data["tx_id"],
                    transaction_data["user_id"],
                    transaction_data["amount"],
                    transaction_data["currency"],
                    transaction_data["merchant"],
                    transaction_data["category"],
                    transaction_data["timestamp"],
                ),
            )
            return True
    except Exception as e:
        logger.error(f"Failed to insert transaction: {e}")
        return False