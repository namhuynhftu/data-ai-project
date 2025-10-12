# Get snowflake connection
# Import data from Minio to stage to snowflake

import os
from io import BytesIO
from pathlib import Path
from typing import Dict, Any

from fastapi import params
from minio import Minio
from minio.error import S3Error
from snowflake.connector import SnowflakeConnection
from elt_pipeline.batch.utils.data_loader import DataLoader
from elt_pipeline.logger_utils import get_snowflake_logger

class SnowflakeLoader(DataLoader):
    """Get Snowflake connection"""

    def __init__(self, params):
        super().__init__(params)
        self.client = None
        self.logger = get_snowflake_logger()
    
    def get_db_connection(self):
        """Establish Snowflake connection."""
        try:
            if self._connection is None:
                self._connection = SnowflakeConnection(
                    user=self.params['user'],
                    password=self.params['password'],
                    account=self.params['account'],
                    warehouse=self.params['warehouse'],
                    database=self.params['database'],
                    schema=self.params['schema']
                )
                # Test connection
                self._connection.cursor().execute("SELECT 1")
                self.logger.info("Snowflake connection established successfully")
            return self._connection
        except Exception as e:
            self.logger.error(f"Snowflake connection failed: {e}")
            raise

    def extract_data(self, sql: str):
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