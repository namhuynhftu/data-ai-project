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
from elt_pipeline.logger_utils import get_minio_logger


class MinIOLoader(DataLoader):
    """MinIO specific data loader implementation."""
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.client = None
        self.logger = get_minio_logger()
    
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
                self.logger.info("MinIO connection established successfully",
                               endpoint=self.params['endpoint'],
                               secure=self.params.get('secure', False))
            return self.client
        except S3Error as e:
            self.logger.error("MinIO connection failed",
                            endpoint=self.params['endpoint'],
                            error=str(e),
                            error_type=type(e).__name__)
            raise
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        pass
    
    def create_bucket(self, bucket_name: str) -> Path:
        """Create a new bucket in MinIO."""
        minio_client = self.get_db_connection()
        try:
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
                self.logger.info("MinIO bucket created", bucket_name=bucket_name)
            else:
                self.logger.info("MinIO bucket already exists", bucket_name=bucket_name)
        except S3Error as e:
            self.logger.error("Error creating MinIO bucket",
                            bucket_name=bucket_name,
                            error=str(e),
                            error_type=type(e).__name__)
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
            self.logger.warning("No data to load to MinIO",
                              bucket=bucket_name,
                              file_name=file_name)
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
        
        self.logger.info("Data loaded to MinIO successfully",
                        bucket=bucket_name,
                        file_name=file_name,
                        file_format=file_format,
                        compression=compression,
                        rows_loaded=len(pd_data),
                        file_size_bytes=data_buffer.getbuffer().nbytes)
        return len(pd_data)

    def get_watermark(self, table_name: str, watermark: str) -> Optional[str]:
        pass