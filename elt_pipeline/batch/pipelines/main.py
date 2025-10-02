import json 
import os
from datetime import datetime

from elt_pipeline.batch.ops.extract_data_from_mysql import load_run_config
from elt_pipeline.batch.ops.extract_data_from_mysql import extract_data_from_mysql
from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio

metadata_path = "elt_pipeline/batch/pipelines/metadata/table_metadata.json"
if __name__ == "__main__":
    with open(metadata_path, "r") as f:
        metadata = json.load(f)
    run_config = load_run_config(metadata_path)
    
    # Process each table in the metadata
    for table_config in run_config["tables"]:
        print(f"Processing table: {table_config.get('source_table')}")
        
        # Create table-specific run config
        table_run_config = {
            **run_config,
            "current_table": table_config
        }
        
        data = extract_data_from_mysql(table_run_config)
        load_data_to_minio(data)
        print(f"Completed processing table: {table_config.get('source_table')}")
    
    print("ETL process completed for all tables.")