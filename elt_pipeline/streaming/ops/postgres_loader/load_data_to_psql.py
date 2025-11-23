from pathlib import Path
import os
import json
from dotenv import load_dotenv
from datetime import datetime
import logging
from typing import Optional, Dict, Any, List
import pandas as pd

load_dotenv()

from elt_pipeline.streaming.utils.psql_loader import PSQLLoader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_psql_config() -> Dict[str, Any]:
    """Load PostgreSQL configuration from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "port": int(os.getenv("POSTGRES_PORT")),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "database": os.getenv("POSTGRES_DB"),  
        "schema": os.getenv("POSTGRES_SCHEMA")
    }


def load_metadata_config(config_path: str) -> Dict[str, Any]:
    """Load metadata configuration from a JSON file."""
    with open(config_path, "r") as f:
        metadata = json.load(f)
    return metadata


def create_table_if_not_exists_op(
    table_name: str,
    metadata_config_path: str = None,
    psql_config: Dict[str, Any] = None
) -> bool:
    """
    Create table in PostgreSQL if it doesn't exist based on metadata configuration
    
    Args:
        table_name: Name of the table to create
        metadata_config_path: Path to metadata configuration file
        psql_config: PostgreSQL connection configuration
    
    Returns:
        True if table was created or already exists, False otherwise
    """
    try:
        # Load configurations
        if metadata_config_path is None:
            metadata_config_path = os.getenv("METADATA_CONFIG_PATH")
        
        if psql_config is None:
            psql_config = load_psql_config()
        
        metadata = load_metadata_config(str(metadata_config_path))
        
        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' not found in metadata configuration")
        
        table_metadata = metadata[table_name]
        
        # Initialize PostgreSQL loader
        psql_loader = PSQLLoader(psql_config)
        
        # Build CREATE TABLE statement
        create_sql = _build_create_table_sql(table_name, table_metadata, psql_config.get("schema"))
        
        # Execute table creation
        engine = psql_loader.get_db_connection()
        with engine.begin() as connection:  # Use begin() for auto-commit transaction
            from sqlalchemy import text
            connection.execute(text(create_sql))
            logger.info(f"Table '{table_name}' created or already exists")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {str(e)}")
        raise


def _build_create_table_sql(table_name: str, table_metadata: Dict[str, Any], schema: str = None) -> str:
    """Build CREATE TABLE SQL statement from metadata configuration"""
    
    # Map metadata types to PostgreSQL types
    type_mapping = {
        "STRING": "VARCHAR",
        "INTEGER": "SERIAL",  # Use SERIAL for auto-incrementing integers
        "DECIMAL(10,2)": "DECIMAL(10,2)",
        "DECIMAL(5,2)": "DECIMAL(5,2)",
        "TIMESTAMP": "TIMESTAMP",
        "FLOAT": "FLOAT",
        "OBJECT": "JSONB"  # Use JSONB for complex objects
    }
    
    columns = []
    primary_keys = []
    
    # Process each column
    for col_name, col_config in table_metadata["columns"].items():
        col_type = col_config["type"]
        
        # For detailed_transactions, skip location and metadata OBJECT types 
        # as they will be flattened into individual columns
        if table_name == "detailed_transactions" and col_name in ["location", "metadata"]:
            continue
        
        # Map type
        if col_type in type_mapping:
            psql_type = type_mapping[col_type]
        elif col_type.startswith("DECIMAL"):
            psql_type = col_type
        else:
            psql_type = "TEXT"  # Default fallback
        
        # Handle max_length for VARCHAR
        if psql_type == "VARCHAR" and "max_length" in col_config:
            psql_type = f"VARCHAR({col_config['max_length']})"
        elif psql_type == "VARCHAR":
            psql_type = "TEXT"  # Use TEXT for unlimited length
        
        # Build column definition
        col_def = f"{col_name} {psql_type}"
        
        # Add NOT NULL constraint
        if not col_config.get("nullable", True):
            col_def += " NOT NULL"
        
        # Check for primary key
        if col_config.get("primary_key", False):
            primary_keys.append(col_name)
        
        columns.append(col_def)
    
    # Add ingested_datetime column if not present
    if "ingested_datetime" not in [col.split()[0] for col in columns]:
        columns.append("ingested_datetime TIMESTAMP NOT NULL")
    
    # Handle detailed_transactions table with flattened columns
    if table_name == "detailed_transactions":
        # Add flattened location columns
        location_columns = [
            "location_city VARCHAR(100)",
            "location_state VARCHAR(100)", 
            "location_country VARCHAR(100)",
            "location_latitude FLOAT",
            "location_longitude FLOAT"
        ]
        columns.extend(location_columns)
        
        # Add flattened metadata columns
        metadata_columns = [
            "metadata_ip_address VARCHAR(45)",
            "metadata_user_agent VARCHAR(500)",
            "metadata_device_type VARCHAR(20)",
            "metadata_channel VARCHAR(20)",
            "metadata_reference_number VARCHAR(20)",
            "metadata_fee_amount DECIMAL(5,2)"
        ]
        columns.extend(metadata_columns)
    
    # Build CREATE TABLE statement with schema
    full_table_name = f"{schema}.{table_name}" if schema else table_name
    create_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"
    create_sql += ",\n".join([f"  {col}" for col in columns])
    
    # Add primary key constraint
    if primary_keys:
        create_sql += f",\n  PRIMARY KEY ({', '.join(primary_keys)})"
    
    create_sql += "\n);"
    
    return create_sql


def load_data_to_psql_op(
    data_result: Dict[str, Any],
    target_table: str = None,
    psql_config: Dict[str, Any] = None,
    metadata_config_path: str = None,
    chunk_size: int = 10000
) -> Dict[str, Any]:
    """
    Load data to PostgreSQL database operation
    
    Args:
        data_result: Result from generate_data operation containing DataFrame and metadata
        target_table: Target table name (if different from data_result table_name)
        psql_config: PostgreSQL connection configuration
        metadata_config_path: Path to metadata configuration file
        chunk_size: Number of rows to insert per chunk
    
    Returns:
        Dictionary containing load results and statistics
    """
    try:
        # Extract data and metadata from data_result
        df = data_result["data"]
        table_name = target_table or data_result["table_name"]
        columns = data_result.get("columns", list(df.columns))
        
        # Load configurations
        if psql_config is None:
            psql_config = load_psql_config()
        
        if metadata_config_path is None:
            metadata_config_path = os.getenv("METADATA_CONFIG_PATH", str(Path(__file__).parent.parent / "config" / "metadata.json"))
        
        logger.info(f"Loading {len(df)} rows to table '{table_name}'")
        logger.info(f"Columns to load: {columns}")
        
        # Create table if not exists
        create_table_if_not_exists_op(table_name, metadata_config_path, psql_config)
        
        # Initialize PostgreSQL loader
        psql_loader = PSQLLoader(psql_config)
        
        # Prepare load parameters
        load_params = {
            "target_tbl": table_name,
            "columns": columns,
            "primary_key": _get_primary_keys(table_name, metadata_config_path),
            "chunk_size": chunk_size
        }
        
        # Load data in chunks
        total_rows = len(df)
        rows_loaded = 0
        chunk_results = []
        
        for i in range(0, total_rows, chunk_size):
            chunk_df = df.iloc[i:i + chunk_size].copy()
            chunk_start = i + 1
            chunk_end = min(i + chunk_size, total_rows)
            
            logger.info(f"Loading chunk {chunk_start}-{chunk_end} of {total_rows}")
            
            try:
                # Load chunk data
                chunk_result = psql_loader.load_data(chunk_df, load_params)
                rows_loaded += len(chunk_df)
                
                chunk_results.append({
                    "chunk_number": len(chunk_results) + 1,
                    "rows_in_chunk": len(chunk_df),
                    "chunk_range": f"{chunk_start}-{chunk_end}",
                    "status": "success"
                })
                
                logger.info(f"Successfully loaded chunk {chunk_start}-{chunk_end}")
                
            except Exception as chunk_error:
                logger.error(f"Error loading chunk {chunk_start}-{chunk_end}: {str(chunk_error)}")
                chunk_results.append({
                    "chunk_number": len(chunk_results) + 1,
                    "rows_in_chunk": len(chunk_df),
                    "chunk_range": f"{chunk_start}-{chunk_end}",
                    "status": "failed",
                    "error": str(chunk_error)
                })
                # Continue with next chunk instead of failing completely
                continue
        
        # Calculate summary statistics
        successful_chunks = [c for c in chunk_results if c["status"] == "success"]
        failed_chunks = [c for c in chunk_results if c["status"] == "failed"]
        
        load_summary = {
            "table_name": table_name,
            "total_rows": total_rows,
            "rows_loaded": rows_loaded,
            "total_chunks": len(chunk_results),
            "successful_chunks": len(successful_chunks),
            "failed_chunks": len(failed_chunks),
            "chunk_size": chunk_size,
            "load_timestamp": datetime.now(),
            "chunk_results": chunk_results
        }
        
        if failed_chunks:
            logger.warning(f"Some chunks failed to load: {len(failed_chunks)} out of {len(chunk_results)}")
        else:
            logger.info(f"All {len(chunk_results)} chunks loaded successfully")
        
        logger.info(f"Load completed: {rows_loaded}/{total_rows} rows loaded to '{table_name}'")
        
        return load_summary
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise


def _get_primary_keys(table_name: str, metadata_config_path: str) -> List[str]:
    """Get primary key columns for a table from metadata configuration"""
    try:
        metadata = load_metadata_config(metadata_config_path)
        table_metadata = metadata.get(table_name, {})
        columns = table_metadata.get("columns", {})
        
        primary_keys = []
        for col_name, col_config in columns.items():
            if col_config.get("primary_key", False):
                primary_keys.append(col_name)
        
        return primary_keys
        
    except Exception as e:
        logger.warning(f"Could not determine primary keys for table '{table_name}': {str(e)}")
        return []


if __name__ == "__main__":
    # Example usage
    logger.info("Testing load_data_to_psql operations...")
    
    # This would typically be called with data from generate_data operation
    # Example usage in pipeline context
    pass