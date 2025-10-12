# Import data from Minio to stage to snowflake
import os 
from typing import Dict, Any
from pathlib  import Path
from io import BytesIO
from datetime import datetime

import pandas as pd
from minio import Minio
from minio.error import S3Error 
from snowflake.connector import SnowflakeConnection

from elt_pipeline.batch.utils.snowflake_loader import SnowflakeLoader
from elt_pipeline.batch.utils.minio_loader import MinIOLoader
from elt_pipeline.logger_utils import get_snowflake_logger, BatchOperation

def load_data_to_snowflake(extracted_data: Dict[str, Any]):
    """Read data from MinIO and load to Snowflake with ingestion_date handling."""
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from dotenv import load_dotenv
    
    load_dotenv()
    logger = get_snowflake_logger()
    
    try:
        # Get MinIO and table configuration
        table_config = extracted_data["table_config"]
        minio_config = extracted_data["minio_target_storage"]
        minio_file_info = extracted_data["minio_file_info"]
        snowflake_target_table = table_config["targets"]["snowflake"]["target_table"]
        
        # Check if we need to add ingestion_date column
        add_ingestion_column = extracted_data.get("add_ingestion_column", False)
        ingestion_date = extracted_data.get("ingestion_date")
        
        with BatchOperation(logger, "loading", destination="snowflake", table=snowflake_target_table) as op:
            
            # Initialize MinIO loader
            minio_loader = MinIOLoader(minio_config)
            minio_client = minio_loader.get_db_connection()
            
            # Get file information from the MinIO loading result
            bucket_name = minio_file_info["bucket"]
            file_path = minio_file_info["file_name"]
            
            logger.info("Reading data from MinIO", 
                       bucket=bucket_name,
                       file_path=file_path,
                       target_table=snowflake_target_table,
                       add_ingestion_column=add_ingestion_column,
                       ingestion_date=ingestion_date)
            
            # Read data from MinIO
            try:
                response = minio_client.get_object(bucket_name, file_path)
                data = response.read()
                df = pd.read_parquet(BytesIO(data))
                
                # Clean column names for Snowflake compatibility
                df.columns = df.columns.str.upper().str.replace('"', '').str.replace("'", "")
                
                # Add ingestion_date column if requested
                if add_ingestion_column and ingestion_date:
                    df['INGESTION_DATE'] = ingestion_date
                    logger.info("Added ingestion_date column",
                               ingestion_date=ingestion_date,
                               column_added='INGESTION_DATE')
                
                logger.info("Data successfully read from MinIO",
                           rows_read=len(df),
                           file_path=file_path,
                           columns=df.columns.tolist())
                
                logger.debug("DataFrame information",
                            columns=df.columns.tolist(),
                            dtypes=df.dtypes.to_dict())
                
                # Convert date columns to proper datetime format
                date_conversions = []
                for col in df.columns:
                    # Check for various timestamp/date column patterns (but skip INGESTION_DATE)
                    is_timestamp_col = any(pattern in col for pattern in ['DATE', 'TIME', '_AT', 'TIMESTAMP', 'CREATED', 'UPDATED', 'MODIFIED'])
                    
                    if is_timestamp_col and col != 'INGESTION_DATE':
                        if df[col].dtype in ['int64', 'float64', 'object']:
                            logger.debug("Converting column to datetime",
                                       column=col,
                                       original_dtype=str(df[col].dtype))
                            try:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                                date_conversions.append(col)
                            except Exception as e:
                                logger.warning("Failed to convert column to datetime",
                                             column=col,
                                             error=str(e))
                        
                        # Convert datetime to string format for Snowflake compatibility
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            logger.debug("Converting datetime column to string for Snowflake", column=col)
                            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                if date_conversions:
                    logger.info("Date column conversions completed", converted_columns=date_conversions)
                
                logger.debug("Final DataFrame information after conversions",
                            dtypes=df.dtypes.to_dict(),
                            columns=list(df.columns))
                            
            except Exception as e:
                logger.error("Failed to read data from MinIO",
                           bucket=bucket_name,
                           file_path=file_path,
                           error=str(e),
                           error_type=type(e).__name__)
                return
            finally:
                response.close()
                response.release_conn()
            
            # Create Snowflake connection with JWT authentication
            logger.debug("Establishing Snowflake connection with JWT authentication")
            
            # Check if JWT authentication is configured
            authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR", "").lower()
            private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
            
            if authenticator == "jwt" and private_key_path:
                # JWT authentication
                logger.info("Using JWT authentication for Snowflake connection")
                
                # Read private key
                with open(private_key_path, "rb") as key_file:
                    private_key = key_file.read()
                
                conn = snowflake.connector.connect(
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    user=os.getenv("SNOWFLAKE_USER"),
                    private_key=private_key,
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    role=os.getenv("SNOWFLAKE_ROLE"),
                )
            else:
                # Password authentication (fallback)
                logger.info("Using password authentication for Snowflake connection")
                conn = snowflake.connector.connect(
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    user=os.getenv("SNOWFLAKE_USER"),
                    password=os.getenv("SNOWFLAKE_PASSWORD"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    role=os.getenv("SNOWFLAKE_ROLE"),
                )
            
            logger.info("Loading data to Snowflake",
                       table=snowflake_target_table,
                       rows_to_load=len(df),
                       database=os.getenv("SNOWFLAKE_DATABASE"),
                       schema=os.getenv("SNOWFLAKE_SCHEMA"),
                       has_ingestion_column=add_ingestion_column)
            
            # Handle ingestion_date column management
            if add_ingestion_column:
                cursor = conn.cursor()
                try:
                    # Check if table exists and if it has INGESTION_DATE column
                    check_column_query = f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = '{os.getenv("SNOWFLAKE_SCHEMA")}' 
                    AND TABLE_NAME = '{snowflake_target_table.upper()}' 
                    AND COLUMN_NAME = 'INGESTION_DATE'
                    """
                    
                    cursor.execute(check_column_query)
                    column_exists = cursor.fetchone()[0] > 0
                    
                    if not column_exists:
                        # Add INGESTION_DATE column if it doesn't exist
                        alter_table_query = f"""
                        ALTER TABLE {os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}.{snowflake_target_table.upper()}
                        ADD COLUMN INGESTION_DATE TIMESTAMP_NTZ
                        """
                        cursor.execute(alter_table_query)
                        logger.info("Added INGESTION_DATE column to table",
                                   table=snowflake_target_table,
                                   column='INGESTION_DATE')
                    else:
                        logger.info("INGESTION_DATE column already exists in table",
                                   table=snowflake_target_table,
                                   column='INGESTION_DATE')
                
                except Exception as e:
                    logger.warning("Failed to check/add INGESTION_DATE column",
                                  table=snowflake_target_table,
                                  error=str(e))
                finally:
                    cursor.close()
            
            # Use Snowflake pandas connector for efficient loading
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=snowflake_target_table.upper(),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                auto_create_table=False, 
                overwrite=True,  
                quote_identifiers=False
            )
            
            # Update operation metrics
            op.records_processed = nrows if success else 0
            
            if success:
                logger.info("Data successfully loaded to Snowflake",
                           table=snowflake_target_table,
                           rows_loaded=nrows,
                           chunks=nchunks)
            else:
                logger.error("Failed to load data to Snowflake",
                           table=snowflake_target_table,
                           attempted_rows=len(df))
                
            conn.close()
            
    except Exception as e:
        logger.error("Snowflake loading operation failed",
                    error=str(e),
                    error_type=type(e).__name__)
        raise  