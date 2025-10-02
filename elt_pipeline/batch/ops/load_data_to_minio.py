# Load data to Minio
import logging
import os
from pathlib import Path
from typing import Dict, Any
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from elt_pipeline.batch.utils.minio_loader import MinIOLoader

def load_data_to_minio(extracted_data: Dict[str, Any]) -> None:
    """Load data to MinIO."""
    minio_loader = MinIOLoader(extracted_data["minio_target_storage"])
    minio_client = minio_loader.get_db_connection()

    # Load data to MinIO based on run configuration
    bucket_name = extracted_data["minio_target_storage"]["bucket"]
    minio_loader.create_bucket(bucket_name)
    
    table_config = extracted_data["table_config"]
    target_tbl = table_config["targets"]["minio"]["target_table"]
    file_format = extracted_data["minio_target_storage"].get("default_format", "parquet")
    compression = extracted_data["minio_target_storage"].get("default_compression", "snappy")

    # Prepare file path and name
    timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    file_name = f"{target_tbl}/{target_tbl}_{timestamp}.{file_format}"

    # Load data to MinIO
    rows_loaded = minio_loader.load_data(extracted_data["data"], {
        "bucket": bucket_name,
        "file_name": file_name,
        "file_format": file_format,
        "compression": compression
    })
    if rows_loaded > 0:
        logging.info(f"Successfully loaded {rows_loaded} rows to MinIO bucket '{bucket_name}' as file '{file_name}'")
    else:
        logging.warning("No data loaded to MinIO")
    return
