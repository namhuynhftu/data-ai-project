"""Debezium CDC connector setup for PostgreSQL."""

import argparse
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
from elt_pipeline.streaming.ops.cdc_connector.debezium_manager import DebeziumSetup

logger = get_logger("cdc_connector.debezium_setup")

def main():
    """Setup Debezium CDC connector."""
    parser = argparse.ArgumentParser(description="Setup Debezium CDC connector")
    parser.add_argument(
        "--snapshot-mode",
        choices=["initial", "always", "never", "schema_only"],
        default="initial",
        help=(
            "Snapshot mode: "
            "initial=snapshot on first run only, "
            "always=snapshot every time, "
            "never=no snapshot, "
            "schema_only=only capture schema"
        ),
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Debezium CDC Setup")
    logger.info(f"Snapshot Mode: {args.snapshot_mode}")
    logger.info("=" * 60)

    setup = DebeziumSetup(snapshot_mode=args.snapshot_mode)

    if not setup.wait_for_connect():
        logger.error("Debezium Connect not available")
        sys.exit(1)

    setup.delete_connector()

    if setup.create_connector():
        logger.info("CDC connector setup complete")
    else:
        logger.error("CDC connector setup failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
