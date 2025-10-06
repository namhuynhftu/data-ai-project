# Import data from Minio to stage to snowflake
import logging
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

def load_data_to_snowflake(extracted_data: Dict[str, Any]):
    """Read data from MinIO and load to Snowflake."""
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    from dotenv import load_dotenv
    
    load_dotenv()
    
    try:
        # Get MinIO and table configuration
        table_config = extracted_data["table_config"]
        minio_config = extracted_data["minio_target_storage"]
        minio_file_info = extracted_data["minio_file_info"]
        
        # Initialize MinIO loader
        minio_loader = MinIOLoader(minio_config)
        minio_client = minio_loader.get_db_connection()
        
        # Get file information from the MinIO loading result
        bucket_name = minio_file_info["bucket"]
        file_path = minio_file_info["file_name"]
        
        logging.info(f"Reading data from MinIO file: {file_path}")
        
        # Read data from MinIO
        try:
            response = minio_client.get_object(bucket_name, file_path)
            data = response.read()
            df = pd.read_parquet(BytesIO(data))
            
            # Clean column names for Snowflake compatibility
            df.columns = df.columns.str.upper().str.replace('"', '').str.replace("'", "")
            
            logging.info(f"Successfully read {len(df)} rows from MinIO file '{file_path}'")
            logging.info(f"DataFrame columns: {df.columns.tolist()}")
            logging.info(f"DataFrame dtypes: {df.dtypes.to_dict()}")
            
            # Convert date columns to proper datetime format
            for col in df.columns:
                # Check for various timestamp/date column patterns
                is_timestamp_col = any(pattern in col for pattern in ['DATE', 'TIME', '_AT', 'TIMESTAMP', 'CREATED', 'UPDATED', 'MODIFIED'])
                
                if is_timestamp_col:
                    if df[col].dtype in ['int64', 'float64', 'object']:
                        logging.info(f"Converting column {col} from {df[col].dtype} to datetime")
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        except Exception as e:
                            logging.warning(f"Failed to convert {col} to datetime: {e}")
                    
                    # Convert datetime to string format for Snowflake compatibility
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        logging.info(f"Converting datetime column {col} to string for Snowflake")
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            logging.info(f"After conversion dtypes: {df.dtypes.to_dict()}")
            logging.info(f"DataFrame columns: {list(df.columns)}")
        except Exception as e:
            logging.error(f"Failed to read from MinIO: {e}")
            return
        finally:
            response.close()
            response.release_conn()
        
        # Create Snowflake connection
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        
        # Get target table name for Snowflake
        snowflake_target_table = table_config["targets"]["snowflake"]["target_table"]
        
        logging.info(f"Loading {len(df)} rows to Snowflake table '{snowflake_target_table}'")
        
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
        
        if success:
            logging.info(f"Successfully loaded {nrows} rows to Snowflake table '{snowflake_target_table}' in {nchunks} chunks")
        else:
            logging.error(f"Failed to load data to Snowflake table '{snowflake_target_table}'")
            
        conn.close()
            
    except Exception as e:
        logging.error(f"Failed to load data from MinIO to Snowflake: {e}")
        raise  