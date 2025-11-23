#!/usr/bin/env python3
"""
Configuration validation script.

Usage: python -m elt_pipeline.streaming.common.validate_config

Exit codes:
    0 - Valid
    1 - Invalid
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from elt_pipeline.streaming.common.config import config
from elt_pipeline.streaming.common.logger import get_logger

logger = get_logger('config.validation')


def validate() -> bool:
    """Validate configuration."""
    errors = []
    
    try:
        # Check Kafka
        if not config.get_kafka_bootstrap_servers():
            errors.append("Kafka bootstrap servers not configured")
        
        # Check Schema Registry
        if not config.get_schema_registry_url():
            errors.append("Schema Registry URL not configured")
        
        # Check PostgreSQL
        pg = config.get_postgres_config()

        
    except Exception as e:
        errors.append(f"Validation error: {e}")
    
    if errors:
        for error in errors:
            logger.error(f"❌ {error}")
        return False
    
    logger.info("✅ Configuration is valid")
    return True


if __name__ == '__main__':
    sys.exit(0 if validate() else 1)