from pathlib import Path
import os
import json
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd

load_dotenv()

from elt_pipeline.batch.utils.mysql_loader import MySQLLoader
from elt_pipeline.batch.utils.loaded_at_tracker import get_table_loaded_at, get_incremental_query_filter
from elt_pipeline.logger_utils import get_mysql_logger, BatchOperation

# Setup centralized logging
logger = get_mysql_logger()



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
        "access_key": os.getenv("MINIO_ACCESS_KEY"),
        "secret_key": os.getenv("MINIO_SECRET_KEY"),
        "bucket": os.getenv("MINIO_BUCKET"),
        "default_format": "parquet",
        "default_compression": "snappy",
        "secure": False  # For local development
    }
    
    # Load Snowflake configuration from environment variables (JWT authentication)
    snowflake_target_storage = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
        "private_key_file_pwd": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA"),  # Default to RAW_DATA schema
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "authenticator": "SNOWFLAKE_JWT"  # Specify JWT authentication
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
    
    # To avoid schema drift issues, use the data contract from pipeline/schema.
    schema_path = Path(__file__).parent.parent / "pipelines" / "schema" / f"{table_name}.json"
    with open(schema_path, "r") as schema_file:
        schema_contract = json.load(schema_file)
        logger.info("Loaded schema contract", table=table_name, schema_path=str(schema_path))
        # Extract column names from the schema contract
        selected_columns = [col["name"] for col in schema_contract.get("columns", [])]
    
    with BatchOperation(logger, "extraction", source="mysql", table=table_name) as op:
        # Initialize MySQL loader with database parameters
        mysql_loader = MySQLLoader(source_db_params)
        
        # Construct SQL query
        sql = f"""
        SELECT {', '.join(selected_columns)}
        FROM {table_config.get("source_table")}
        WHERE 1=1
    """
        
        # Choose extract strategy 
        strategy = table_config.get("strategy", "full_load")
        loaded_at = None
        new_watermark = None
        is_first_incremental_load = False
        
        if strategy == "incremental_by_watermark":
            # Get load_from and load_at from loaded_at.json
            load_from, load_at = get_incremental_query_filter(table_name)
            watermark_column = table_config.get("watermark_column")
            
            if load_from is None:
                # First incremental load: load data from beginning up to load_at
                is_first_incremental_load = True
                logger.info("First incremental load: loading from beginning to load_at",
                           table=table_name, 
                           strategy=strategy,
                           load_at=load_at)
                if watermark_column and load_at:
                    sql += f" AND {watermark_column} <= '{load_at}'"
                    loaded_at = load_at
                    # Set new_watermark to current timestamp for next load
                    new_watermark = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Subsequent incremental load: load data from load_from to current time
                current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"Incremental load from {load_from} to current time", 
                           table=table_name, 
                           strategy=strategy, 
                           load_from=load_from,
                           load_to=current_timestamp)
                if watermark_column:
                    sql += f" AND {watermark_column} > '{load_from}'"
                    loaded_at = load_from
                    new_watermark = current_timestamp
        elif strategy == "full_load":
            logger.info("Performing full load", table=table_name, strategy=strategy)

        logger.debug("Executing SQL query", table=table_name, sql=sql)
        
        # Extract data
        pd_data = mysql_loader.extract_data(sql)
        
        # Update operation metrics
        op.records_processed = len(pd_data) if pd_data is not None else 0
        
        logger.info("Data extraction completed successfully",
                   table=table_name,
                   records_extracted=op.records_processed,
                   strategy=strategy)

        return {
            "data": pd_data,
            "table_config": table_config,
            "loaded_at": loaded_at,
            "new_watermark": new_watermark,  # Pass the new watermark for updating loaded_at.json
            "is_first_incremental_load": is_first_incremental_load,  # Flag for truncate behavior
            "minio_target_storage": run_config["minio_target_storage"],
            "snowflake_target_storage": run_config["snowflake_target_storage"]
        }

