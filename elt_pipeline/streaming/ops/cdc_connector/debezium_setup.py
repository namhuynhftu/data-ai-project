"""Debezium CDC connector setup for PostgreSQL."""

import os
import sys
import time
import requests
from pathlib import Path
from typing import Dict, Any

from requests import Response
# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from elt_pipeline.streaming.common.logger import get_logger

logger = get_logger("cdc_connector.debezium_setup")


class DebeziumSetup:
    """Configure Debezium PostgreSQL CDC connector."""

    def __init__(
        self,
        connect_url: str = "http://localhost:8083",
        connector_name: str = "postgres-cdc-connector",
    ):
        self.connect_url = connect_url
        self.connector_name = connector_name

    def wait_for_connect(self, max_retries: int = 30) -> bool:
        """Wait for Debezium Connect to be ready."""
        logger.info("Waiting for Debezium Connect...")
        for i in range(max_retries):
            try:
                response = requests.get(f"{self.connect_url}/connectors")
                if response.status_code == 200:
                    logger.info("✅ Debezium Connect is ready")
                    return True
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        return False

    def delete_connector(self) -> bool:
        """Delete existing connector if exists."""
        try:
            response = requests.delete(
                f"{self.connect_url}/connectors/{self.connector_name}"
            )
            if response.status_code == 204:
                logger.info(f"Deleted existing connector: {self.connector_name}")
            return True
        except Exception as e:
            logger.warning(f"No existing connector to delete: {e}")
            return True

    def create_connector(self) -> bool:
        """Create PostgreSQL CDC connector."""
        config = self._get_connector_config()

        try:
            response = requests.post(
                f"{self.connect_url}/connectors",
                json=config,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            logger.info(f"✅ Created CDC connector: {self.connector_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create connector: {e}")
            logger.error(f"Response: {response.text if 'response' in locals() else 'N/A'}")
            return False

    def _get_connector_config(self) -> Dict[str, Any]:
        """Build Debezium connector configuration."""
        return {
            "name": self.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "postgres_streaming",
                "database.port": "5432",
                "database.user": os.getenv("POSTGRES_USER", "user"),
                "database.password": os.getenv("POSTGRES_PASSWORD", "password"),
                "database.dbname": os.getenv("POSTGRES_DB", "streaming_db"),
                "topic.prefix": "postgres",
                "table.include.list": "streaming.users,streaming.transactions,streaming.detailed_transactions",
                "plugin.name": "pgoutput",
                "slot.name": "debezium_slot",
                "publication.name": "debezium_publication",
                "snapshot.mode": "initial",
            },
        }


def main():
    """Setup Debezium CDC connector."""
    logger.info("=" * 60)
    logger.info("Debezium CDC Setup")
    logger.info("=" * 60)

    setup = DebeziumSetup()

    if not setup.wait_for_connect():
        logger.error("❌ Debezium Connect not available")
        sys.exit(1)

    setup.delete_connector()

    if setup.create_connector():
        logger.info("✅ CDC connector setup complete")
    else:
        logger.error("❌ CDC connector setup failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
