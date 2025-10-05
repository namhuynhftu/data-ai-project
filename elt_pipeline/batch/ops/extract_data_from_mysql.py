from pathlib import Path
import os
import json
from dotenv import load_dotenv
from datetime import datetime
import logging
from typing import Optional, Dict, Any, List
import pandas as pd

load_dotenv()

from elt_pipeline.batch.utils.mysql_loader import MySQLLoader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def load_run_config(config_path: str) -> Dict[str, Any]:
    """Load run configuration from a JSON file."""
    with open(config_path, "r") as f:
        metadata = json.load(f)
    
    
    tables = metadata.get("tables", [])
    data_source_config = {
        "host": os.getenv("MYSQL_HOST"),
        "port": int(os.getenv("MYSQL_PORT", 3306)), 
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE"),
        "schema": os.getenv("MYSQL_SCHEMA")
    }
    
    # Load MinIO configuration from environment variables
    minio_target_storage = {
        "endpoint": os.getenv("MINIO_ENDPOINT"),
        "access_key": os.getenv("MINIO_ROOT_USER"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
        "bucket": os.getenv("MINIO_BUCKET"),
        "default_format": "parquet",
        "default_compression": "snappy",
        "secure": False  # For local development
    }
    
    # Load Snowflake configuration from environment variables
    snowflake_target_storage = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA"),  # Default to RAW_DATA schema
        "role": os.getenv("SNOWFLAKE_ROLE")
    }

    return {
        "tables": tables, 
        "data_source_config": data_source_config, 
        "minio_target_storage": minio_target_storage, 
        "snowflake_target_storage": snowflake_target_storage
    }

def extract_data_from_mysql(run_config) -> Dict[str, Any]:
    """Extract data from MySQL database based on run configuration."""
    source_db_params = run_config["data_source_config"]
    table_config = run_config["current_table"]
    
    # Initialize MySQL loader with database parameters
    mysql_loader = MySQLLoader(source_db_params)
    
    # Construct SQL query
    sql = f"""
    SELECT * 
    FROM {table_config.get("source_table")}
    WHERE 1=1
"""
    # Choose extract strategy 
    loaded_at = table_config.get("loaded_at", "1970-01-01 00:00:00")
    strategy = table_config.get("strategy", "full_load")
    if strategy == "incremental_by_watermark":
        if loaded_at is None: 
            logger.info("Incremental load for first time, performing full load")
            watermark_column = table_config.get("watermark_column")
            if watermark_column:
                watermark_value = mysql_loader.get_watermark(table_config.get("source_table"), watermark_column)
            loaded_at = watermark_value if watermark_value is not None else "1970-01-01 00:00:00"
        else:
            logger.info(f"Incremental load since {loaded_at}")
            watermark_column = table_config.get("watermark_column")
            if watermark_column:
                sql += f" AND {watermark_column} > '{loaded_at}'"
    elif strategy == "full_load":
        logger.info("Performing full load")

    logger.info(f"Executing SQL: {sql}")
    
    # Extract data
    pd_data = mysql_loader.extract_data(sql)

    return {
        "data": pd_data,
        "table_config": table_config,
        "loaded_at": loaded_at,
        "minio_target_storage": run_config["minio_target_storage"],
        "snowflake_target_storage": run_config["snowflake_target_storage"]
    }

