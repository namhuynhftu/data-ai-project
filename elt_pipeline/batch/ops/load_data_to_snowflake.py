"""
MinIO to Snowflake data loading with temp file fallback for local development.
"""
import os 
from typing import Dict, Any
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from elt_pipeline.batch.utils.snowflake_loader import SnowflakeLoader
from elt_pipeline.logger_utils import get_snowflake_logger, BatchOperation

load_dotenv()


def load_minio_to_snowflake_via_stage_direct(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Complete workflow: Load data from MinIO to Snowflake via staging with temp file fallback.
    """
    logger = get_snowflake_logger()
    table_name = extracted_data["table_config"]["targets"]["snowflake"]["target_table"]
    
    logger.info("Loading to Snowflake", table=table_name)
    
    try:
        # Phase 1: Load to Snowflake Stage
        stage_result = load_data_to_snowflake_stage_direct(extracted_data)
        
        if not stage_result["success"]:
            raise Exception("Failed to load data to Snowflake stage")
        
        # Phase 2: Load from Stage to Table
        if not stage_result.get("already_loaded_to_table", False):
            table_result = load_data_from_snowflake_stage_to_table(extracted_data, stage_result)
            
            if not table_result["success"]:
                raise Exception("Failed to load data from stage to table")
        else:
            table_result = {
                "success": True, 
                "rows_loaded": stage_result.get("rows_staged", 0), 
                "files_processed": stage_result.get("files_copied", 0)
            }
        
        # Return results
        return {
            "success": True,
            "total_rows_loaded": table_result["rows_loaded"],
            "stage_name": stage_result["stage_name"],
            "method": stage_result.get("method", "temp_file_fallback")
        }
        
    except Exception as e:
        logger.error("Failed to load to Snowflake", table=table_name, error=str(e))
        raise




def load_data_to_snowflake_stage_direct(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load data to Snowflake stage with temp file fallback for localhost environments.
    """
    logger = get_snowflake_logger()
    table_name = extracted_data["table_config"]["targets"]["snowflake"]["target_table"]
    
    try:
        # Try direct MinIO loading first, fallback to temp files
        try:
            return _load_direct_from_minio(extracted_data, logger)
        except Exception:
            logger.info("Using temp file fallback", table=table_name)
            return _load_via_temp_file(extracted_data, logger)
    
    except Exception as e:
        logger.error("Stage loading failed", table=table_name, error=str(e))
        raise
            
def _load_direct_from_minio(extracted_data: Dict[str, Any], logger):
    """Direct MinIO loading - not supported for localhost development"""
    raise Exception("Direct MinIO loading not supported for localhost. Using temp file fallback.")




def load_data_from_snowflake_stage_to_table(extracted_data: Dict[str, Any], stage_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load data from Snowflake stage to final table with overwrite mode.
    """
    logger = get_snowflake_logger()
    
    try:
        # Get configuration
        table_config = extracted_data["table_config"]
        snowflake_target_table = table_config["targets"]["snowflake"]["target_table"]
        stage_name = stage_info["stage_name"]
        database = stage_info["database"]
        schema = stage_info["schema"]
        internal_snowflake_path = stage_info.get("internal_snowflake_path")
        
        # Check if we need to add ingestion_date column
        add_ingestion_column = extracted_data.get("add_ingestion_column", False)
        ingestion_date = extracted_data.get("ingestion_date")
        
        with BatchOperation(logger, "stage_to_table", destination="snowflake", table=snowflake_target_table) as op:
            
            # Create Snowflake configuration for the loader
            snowflake_config = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
                "private_key_file_pwd": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
                "role": os.getenv("SNOWFLAKE_ROLE")
            }
            
            # Initialize SnowflakeLoader
            snowflake_loader = SnowflakeLoader(snowflake_config)
            conn = snowflake_loader.get_db_connection()
            cursor = conn.cursor()
            
            try:
                # Truncate table for overwrite mode
                truncate_sql = f"TRUNCATE TABLE {database}.{schema}.{snowflake_target_table.upper()}"
                cursor.execute(truncate_sql)
                logger.info("Table truncated", table=snowflake_target_table)
                
                # Check if ingestion_date column exists, add if needed
                if add_ingestion_column and ingestion_date:
                    check_column_query = f"""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{snowflake_target_table.upper()}' 
                    AND COLUMN_NAME = 'INGESTION_DATE'
                    """
                    cursor.execute(check_column_query)
                    column_exists = cursor.fetchone()[0] > 0
                    
                    if not column_exists:
                        alter_table_query = f"""
                        ALTER TABLE {database}.{schema}.{snowflake_target_table.upper()}
                        ADD COLUMN INGESTION_DATE TIMESTAMP_NTZ
                        """
                        cursor.execute(alter_table_query)
                
                # Execute COPY INTO from stage to table
                table_subfolder = snowflake_target_table.lower()
                copy_sql = f"""
                COPY INTO {database}.{schema}.{snowflake_target_table.upper()}
                FROM @{stage_name}/{table_subfolder}/{internal_snowflake_path}
                FILE_FORMAT = (
                    TYPE = 'PARQUET'
                    USE_LOGICAL_TYPE = TRUE
                    BINARY_AS_TEXT = FALSE
                )
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE'
                """
                
                cursor.execute(copy_sql)
                copy_results = cursor.fetchall()
                
                # Parse COPY INTO results
                rows_loaded = 0
                files_processed = 0
                for result in copy_results:
                    rows_loaded += result[3]  # rows_loaded from each file
                    files_processed += 1
                
                logger.info("Data copied to table", 
                           table=snowflake_target_table,
                           rows=rows_loaded,
                           files=files_processed)
                
                # Update ingestion_date column if requested
                if add_ingestion_column and ingestion_date and rows_loaded > 0:
                    update_sql = f"""
                    UPDATE {database}.{schema}.{snowflake_target_table.upper()}
                    SET INGESTION_DATE = '{ingestion_date}'
                    WHERE INGESTION_DATE IS NULL
                    """
                    cursor.execute(update_sql)
                
                # Update operation metrics
                op.records_processed = rows_loaded
                
                return {
                    "rows_loaded": rows_loaded,
                    "files_processed": files_processed,
                    "success": True
                }
                
            finally:
                cursor.close()
                conn.close()
                
    except Exception as e:
        logger.error("Loading from stage to table failed", 
                    table=snowflake_target_table, 
                    error=str(e))
        raise


def _load_via_temp_file(extracted_data: Dict[str, Any], logger) -> Dict[str, Any]:
    """
    Fallback method: Download data from MinIO to temp file, then load to Snowflake stage
    """
    from minio import Minio
    
    table_config = extracted_data["table_config"]
    minio_config = extracted_data["minio_target_storage"]
    minio_file_info = extracted_data["minio_file_info"]
    snowflake_target_table = table_config["targets"]["snowflake"]["target_table"]
    file_name = f"{snowflake_target_table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    with BatchOperation(logger, "temp_file_load", destination="snowflake_stage", table=snowflake_target_table) as op:
        temp_file_path = None
        
        try:
            # Create temp directory and file path
            temp_dir = Path("data_temp")
            temp_dir.mkdir(exist_ok=True)
            temp_file_path = temp_dir / file_name
            
            # Download from MinIO to temp file
            _download_from_minio_to_temp(minio_config, minio_file_info, temp_file_path, logger)
            
            # Load temp file to Snowflake stage
            stage_result = _load_temp_file_to_snowflake_stage(temp_file_path, snowflake_target_table, logger)
            
            # Update operation metrics
            op.records_processed = minio_file_info.get("rows_loaded", 0)
            
            return stage_result
            
        finally:
            # Clean up temp file
            if temp_file_path and temp_file_path.exists():
                try:
                    temp_file_path.unlink()
                except Exception:
                    pass  # Silent cleanup failure for MVP


def _download_from_minio_to_temp(minio_config: Dict[str, Any], minio_file_info: Dict[str, Any], 
                                temp_file_path: Path, logger):
    """Download file from MinIO to local temp file"""
    from minio import Minio
    
    # Parse MinIO endpoint
    endpoint_val = minio_config.get("endpoint")
    if endpoint_val and ":" in endpoint_val:
        host, port = endpoint_val.split(":", 1)
    else:
        host = minio_config.get("host", "localhost")
        port = minio_config.get("port", "9000")
    
    # Create MinIO client
    minio_client = Minio(
        f"{host}:{port}",
        access_key=minio_config.get("access_key"),
        secret_key=minio_config.get("secret_key"),
        secure=minio_config.get("secure", False)
    )
    
    # Download file
    minio_client.fget_object(
        minio_file_info["bucket"], 
        minio_file_info["file_name"], 
        str(temp_file_path)
    )


def _load_temp_file_to_snowflake_stage(temp_file_path: Path, table_name: str, logger) -> Dict[str, Any]:
    """Load temp file to Snowflake internal stage using PUT command"""
    
    # Create Snowflake configuration
    snowflake_config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
        "private_key_file_pwd": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE")
    }
    
    # Initialize SnowflakeLoader
    snowflake_loader = SnowflakeLoader(snowflake_config)
    conn = snowflake_loader.get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Create internal stage if not exists
        stage_name = "MINIO_STAGE_SHARED"
        create_stage_sql = f"""
        CREATE STAGE IF NOT EXISTS {os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}.{stage_name}
        FILE_FORMAT = (
            TYPE = 'PARQUET'
            USE_LOGICAL_TYPE = TRUE
            BINARY_AS_TEXT = FALSE
        )
        COMMENT = 'Shared internal staging area for all tables'
        """
        cursor.execute(create_stage_sql)
        
        # Upload file to stage using PUT
        table_subfolder = table_name.lower()
        internal_snowflake_path = f'{table_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}'.lower()
        put_sql = f"PUT file://{temp_file_path.as_posix()} @{stage_name}/{table_subfolder}/{internal_snowflake_path}"
        cursor.execute(put_sql)
        put_results = cursor.fetchall()
        
        # Count uploaded files
        files_uploaded = len(put_results)
        
        logger.info("File uploaded to stage", 
                   table=table_name,
                   stage=stage_name,
                   files=files_uploaded)
        
        return {
            "stage_name": stage_name,
            "rows_staged": 0,  # Will be counted during COPY INTO
            "success": True,
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "method": "temp_file_upload",
            "already_loaded_to_table": False,
            "internal_snowflake_path": internal_snowflake_path,
            "files_copied": files_uploaded
        }
        
    finally:
        cursor.close()
        conn.close()




