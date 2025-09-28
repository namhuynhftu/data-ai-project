import logging
import pandas as pd
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from typing import Dict, Any, Optional
from .data_loader import DataLoader


class MinIOLoader(DataLoader):
    """MinIO specific data loader implementation."""
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_db_connection(self) -> Minio:
        """Establish MinIO connection."""
        try:
            self.client = Minio(
                endpoint=self.params['endpoint'],
                access_key=self.params['access_key'],
                secret_key=self.params['secret_key'],
                secure=self.params.get('secure', False)  # False for local development
            )
            
            # Test connection
            self.client.list_buckets()
            self.logger.info("MinIO connection established successfully")
            return self.client
            
        except Exception as e:
            self.logger.error(f"MinIO connection failed: {e}")
            raise
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        """Not applicable for MinIO - used for object storage."""
        raise NotImplementedError("MinIO is for storage, not data extraction")
    
    def load_data(self, pd_data: pd.DataFrame, params: Dict[str, Any]) -> int:
        """Load DataFrame to MinIO as parquet file."""
        if pd_data is None or len(pd_data) == 0:
            self.logger.warning("No data to load to MinIO")
            return 0
        
        try:
            bucket_name = params['bucket_name']
            object_path = params['object_path']
            
            # Ensure bucket exists
            self._ensure_bucket_exists(bucket_name)
            
            # Convert DataFrame to parquet bytes
            parquet_buffer = BytesIO()
            pd_data.to_parquet(
                parquet_buffer,
                index=False,
                compression='gzip',
                engine='pyarrow'
            )
            parquet_buffer.seek(0)
            
            # Upload to MinIO
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_path,
                data=parquet_buffer,
                length=len(parquet_buffer.getvalue()),
                content_type='application/octet-stream'
            )
            
            self.logger.info(f"Successfully loaded {len(pd_data)} rows to MinIO: s3://{bucket_name}/{object_path}")
            return len(pd_data)
            
        except Exception as e:
            self.logger.error(f"Failed to load data to MinIO: {e}")
            raise
    
    def _ensure_bucket_exists(self, bucket_name: str) -> None:
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                self.logger.info(f"Created bucket: {bucket_name}")
            else:
                self.logger.info(f"Bucket already exists: {bucket_name}")
                
        except S3Error as e:
            self.logger.error(f"Error managing bucket {bucket_name}: {e}")
            raise
    
    def get_watermark(self, table_name: str, watermark: str) -> Optional[str]:
        """Not applicable for MinIO object storage."""
        return None
    
    def list_objects(self, bucket_name: str, prefix: str = "") -> list:
        """List objects in MinIO bucket."""
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix)
            return [obj.object_name for obj in objects]
        except Exception as e:
            self.logger.error(f"Failed to list objects in bucket {bucket_name}: {e}")
            raise