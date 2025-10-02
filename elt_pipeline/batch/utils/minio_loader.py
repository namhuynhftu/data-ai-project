import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from minio import Minio
from minio.error import S3Error

from elt_pipeline.batch.utils.data_loader import DataLoader


class MinIOLoader(DataLoader):
    """MinIO specific data loader implementation."""
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_db_connection(self) -> Minio:
        """Establish MinIO connection."""
        try:
            if self.client is None:
                self.client = Minio(
                    endpoint=self.params['endpoint'],
                    access_key=self.params['access_key'],
                    secret_key=self.params['secret_key'],
                    secure=self.params.get('secure', False)
                )
                # Test connection
                self.client.list_buckets()
                self.logger.info("MinIO connection established successfully")
            return self.client
        except S3Error as e:
            self.logger.error(f"MinIO connection failed: {e}")
            raise
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        pass
    
    def create_bucket(self, bucket_name: str) -> Path:
        """Create a new bucket in MinIO."""
        minio_client = self.get_db_connection()
        try:
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
                self.logger.info(f"Bucket created: {bucket_name}")
            else:
                self.logger.info(f"Bucket already exists: {bucket_name}")
        except S3Error as e:
            self.logger.error(f"Error creating bucket {bucket_name}: {e}")
            raise
        return Path(bucket_name)
            
    def load_data(self, pd_data: pd.DataFrame, params: Dict[str, Any]) -> int:
        """Load DataFrame to MinIO bucket"""
        minio_client = self.get_db_connection()
        bucket_name = params['bucket']
        file_name = params['file_name']
        file_format = params.get('file_format', 'parquet')
        compression = params.get('compression', 'snappy')

        if pd_data is None or pd_data.empty:
            self.logger.warning("No data to load to MinIO")
            return 0
        
        # Convert DataFrame to Parquet in memory    
        data_buffer = BytesIO() 
        table = pa.Table.from_pandas(pd_data)
        pq.write_table(table, data_buffer, compression=compression, )
        data_buffer.seek(0)

        # Upload to MinIO
        minio_client.put_object(
            bucket_name, 
            file_name, 
            data_buffer, 
            data_buffer.getbuffer().nbytes, 
            content_type=f"application/{file_format}")
        self.logger.info(f"Data loaded to MinIO bucket '{bucket_name}' as file '{file_name}'")
        return 1

    def get_watermark(self, table_name: str, watermark: str) -> Optional[str]:
        pass