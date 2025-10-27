"""
Utility module for tracking incremental load timestamps.
Manages the loaded_at.json file that tracks load_from and load_at for incremental loads.
"""
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Any
from elt_pipeline.logger_utils import get_mysql_logger

logger = get_mysql_logger()

LOADED_AT_FILE = "elt_pipeline/batch/pipelines/metadata/loaded_at.json"


def load_loaded_at_metadata() -> Dict[str, Any]:
    """
    Load the loaded_at.json metadata file.
    
    Returns:
        Dict containing the loaded_at metadata
    """
    try:
        with open(LOADED_AT_FILE, 'r') as f:
            metadata = json.load(f)
            logger.debug("Loaded loaded_at.json metadata", 
                        tables_tracked=len(metadata.get("tables", {})))
            return metadata
    except FileNotFoundError:
        logger.warning("loaded_at.json not found, returning empty metadata")
        return {"tables": {}, "last_updated": None}
    except json.JSONDecodeError as e:
        logger.error("Failed to parse loaded_at.json", error=str(e))
        raise


def get_table_loaded_at(table_name: str) -> Dict[str, Optional[str]]:
    """
    Get the load_from and load_at timestamps for a specific table.
    
    Args:
        table_name: Name of the table to get timestamps for
        
    Returns:
        Dict with 'load_from' and 'load_at' keys
    """
    metadata = load_loaded_at_metadata()
    table_data = metadata.get("tables", {}).get(table_name, {})
    
    load_from = table_data.get("load_from")
    load_at = table_data.get("load_at")
    
    logger.debug("Retrieved loaded_at info", 
                table=table_name,
                load_from=load_from,
                load_at=load_at)
    
    return {
        "load_from": load_from,
        "load_at": load_at
    }


def update_table_loaded_at(table_name: str, new_watermark: Optional[str] = None) -> None:
    """
    Update the loaded_at.json file after a successful load.
    Sets load_from = previous load_at, and load_at = new_watermark (or current timestamp).
    
    Args:
        table_name: Name of the table to update
        new_watermark: The new watermark value (timestamp). If None, uses current timestamp.
    """
    metadata = load_loaded_at_metadata()
    
    if "tables" not in metadata:
        metadata["tables"] = {}
    
    # Get current values
    current_data = metadata["tables"].get(table_name, {})
    current_load_at = current_data.get("load_at")
    
    # Update timestamps
    new_load_from = current_load_at  # Previous load_at becomes new load_from
    new_load_at = new_watermark if new_watermark else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Update the metadata
    metadata["tables"][table_name] = {
        **current_data,
        "load_from": new_load_from,
        "load_at": new_load_at
    }
    metadata["last_updated"] = datetime.now().isoformat()
    
    # Write back to file
    try:
        with open(LOADED_AT_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info("Updated loaded_at metadata", 
                   table=table_name,
                   load_from=new_load_from,
                   load_at=new_load_at)
    except Exception as e:
        logger.error("Failed to update loaded_at.json", 
                    table=table_name,
                    error=str(e))
        raise


def initialize_table_loaded_at(
    table_name: str, 
    initial_load_at: str,
    watermark_column: str,
    strategy: str = "incremental_by_watermark"
) -> None:
    """
    Initialize a new table entry in loaded_at.json.
    
    Args:
        table_name: Name of the table
        initial_load_at: Initial load_at timestamp (e.g., "2018-10-01 00:00:00")
        watermark_column: Name of the watermark column
        strategy: Load strategy (default: "incremental_by_watermark")
    """
    metadata = load_loaded_at_metadata()
    
    if "tables" not in metadata:
        metadata["tables"] = {}
    
    # Only initialize if table doesn't exist
    if table_name not in metadata["tables"]:
        metadata["tables"][table_name] = {
            "load_from": None,
            "load_at": initial_load_at,
            "watermark_column": watermark_column,
            "strategy": strategy
        }
        metadata["last_updated"] = datetime.now().isoformat()
        
        with open(LOADED_AT_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info("Initialized table in loaded_at.json", 
                   table=table_name,
                   initial_load_at=initial_load_at)
    else:
        logger.debug("Table already initialized in loaded_at.json", table=table_name)


def get_incremental_query_filter(table_name: str) -> tuple[Optional[str], Optional[str]]:
    """
    Get the SQL filter condition for incremental load based on loaded_at metadata.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Tuple of (load_from, load_at) timestamps to use in query
    """
    loaded_at_info = get_table_loaded_at(table_name)
    load_from = loaded_at_info.get("load_from")
    load_at = loaded_at_info.get("load_at")
    
    return load_from, load_at
