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

def get_state_file_path(table_name: str) -> str:
    """Get the state file path for a table."""
    state_dir = "elt_pipeline/batch/state"
    os.makedirs(state_dir, exist_ok=True)
    return os.path.join(state_dir, f"{table_name}_state.json")

def load_table_state(table_name: str) -> Optional[str]:
    """Load the last loaded timestamp for a table."""
    state_file = get_state_file_path(table_name)
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                last_loaded = state.get('last_loaded_at')
                logger.info(f"Loaded state for {table_name}: last_loaded_at = {last_loaded}")
                return last_loaded
        except Exception as e:
            logger.warning(f"Could not load state for {table_name}: {e}")
    return None

def save_table_state(table_name: str, loaded_at: str, row_count: int = 0):
    """Save the last loaded timestamp for a table."""
    state_file = get_state_file_path(table_name)
    state = {
        'table_name': table_name,
        'last_loaded_at': loaded_at,
        'row_count': row_count,
        'updated_at': datetime.now().isoformat()
    }
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Saved state for {table_name}: last_loaded_at = {loaded_at}")
    except Exception as e:
        logger.error(f"Could not save state for {table_name}: {e}")



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
    table_name = table_config.get("source_table")
    
    # Initialize MySQL loader with database parameters
    mysql_loader = MySQLLoader(source_db_params)
    
    # Construct SQL query
    sql = f"""
    SELECT * 
    FROM {table_name}
    WHERE 1=1
"""
    
    # Load the last loaded timestamp from state
    strategy = table_config.get("strategy", "full_load")
    loaded_at = None
    new_watermark = None
    
    if strategy == "incremental_by_watermark":
        # Load previous state
        loaded_at = load_table_state(table_name)
        watermark_column = table_config.get("watermark_column")
        
        if loaded_at is None:
            logger.info(f"Incremental load for {table_name} - first time, performing full load")
            # Get the current max watermark to save as state after successful load
            if watermark_column:
                new_watermark = mysql_loader.get_watermark(table_name, watermark_column)
        else:
            logger.info(f"Incremental load for {table_name} since {loaded_at}")
            if watermark_column:
                sql += f" AND {watermark_column} > '{loaded_at}'"
                # Get the new max watermark for this incremental load
                new_watermark = mysql_loader.get_watermark(table_name, watermark_column)
    elif strategy == "full_load":
        logger.info(f"Performing full load for {table_name}")

    logger.info(f"Executing SQL: {sql}")
    
    # Extract data
    pd_data = mysql_loader.extract_data(sql)

    return {
        "data": pd_data,
        "table_config": table_config,
        "table_name": table_name,
        "loaded_at": loaded_at,
        "new_watermark": new_watermark,
        "strategy": strategy,
        "minio_target_storage": run_config["minio_target_storage"],
        "snowflake_target_storage": run_config["snowflake_target_storage"]
    }

