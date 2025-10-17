# Get snowflake connection
# Import data from Minio to stage to snowflake

import os
from io import BytesIO
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

import snowflake.connector
from fastapi import params
from minio import Minio
from minio.error import S3Error
from elt_pipeline.batch.utils.data_loader import DataLoader
from elt_pipeline.logger_utils import get_snowflake_logger

class SnowflakeLoader(DataLoader):
    """Get Snowflake connection"""

    def __init__(self, params):
        super().__init__(params)
        self.client = None
        self.logger = get_snowflake_logger()
    
    def get_db_connection(self):
        """Establish Snowflake connection using JWT authentication with keypair."""
        try:
            if self._connection is None:
                # Load environment variables
                load_dotenv()
                
                self._connection = snowflake.connector.connect(
                    account=os.getenv("SNOWFLAKE_ACCOUNT") or self.params.get('account'),
                    user=os.getenv("SNOWFLAKE_USER") or self.params.get('user'),
                    private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH") or self.params.get('private_key_file'),
                    private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD") or self.params.get('private_key_file_pwd'),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE") or self.params.get('warehouse'),
                    database=os.getenv("SNOWFLAKE_DATABASE") or self.params.get('database'),
                    schema=os.getenv("SNOWFLAKE_SCHEMA") or self.params.get('schema', 'RAW_DATA'),
                    role=os.getenv("SNOWFLAKE_ROLE") or self.params.get('role'),
                    authenticator='SNOWFLAKE_JWT'
                )
                
                # Test connection
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                
                self.logger.info("Snowflake connection established successfully using JWT authentication")
            return self._connection
        except Exception as e:
            self.logger.error(f"Snowflake connection failed: {e}")
            raise

    def extract_data(self, sql: str):
        pass

    def create_internal_stage(self, stage_name: str, conn):
        """Create an internal stage in Snowflake."""
        pass 
    
    def load_data(self, pd_data, conn):
        """Load data into Snowflake database"""
        try:
            if pd_data is None or len(pd_data) == 0:
                self.logger.warning("No data to load to Snowflake")
                return 0
            
            # Create a temporary CSV file in memory
            csv_buffer = BytesIO()
            pd_data.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            # Upload the CSV file to a Snowflake stage
            stage_name = self.params.get('stage_name', 'my_stage')
            table_name = self.params.get('target_tbl')
            conn.cursor().execute(f"PUT file://{csv_buffer} @{stage_name}")
            
            # Copy data from the stage to the target table
            copy_query = f"""
                COPY INTO {table_name}
                FROM @{stage_name}
                FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
                ON_ERROR = 'CONTINUE';
            """
            conn.cursor().execute(copy_query)
            
            self.logger.info(f"Data loaded into Snowflake table {table_name} successfully")
            return len(pd_data)
        except Exception as e:
            self.logger.error(f"Data load to Snowflake failed: {e}")
            raise