import json 
import os
from datetime import datetime
import pandas as pd

from elt_pipeline.batch.ops.load_data_to_snowflake import load_data_to_snowflake
from elt_pipeline.batch.ops.extract_data_from_mysql import load_run_config
from elt_pipeline.batch.ops.extract_data_from_mysql import extract_data_from_mysql
from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
from elt_pipeline.logger_utils import BatchLogger, BatchOperation

metadata_path = "elt_pipeline/batch/pipelines/metadata/table_metadata.json"

if __name__ == "__main__":
    logger = BatchLogger("batch_pipeline")
    
    logger.info("🚀 Starting batch ETL pipeline", 
               metadata_path=metadata_path,
               start_time=datetime.now().isoformat())
    
    with open(metadata_path, "r") as f:
        metadata = json.load(f)
    run_config = load_run_config(metadata_path)
    
    total_tables = len(run_config["tables"])
    logger.info("Pipeline configuration loaded", 
               total_tables=total_tables,
               tables=[table.get('source_table') for table in run_config["tables"]])
    
    # Current ingestion timestamp for this pipeline run
    ingestion_date = datetime.now().isoformat()
    
    # PHASE 1: Extract all data to MinIO first
    logger.info("🔄 PHASE 1: Starting data extraction to MinIO", 
               phase="extraction_phase",
               total_tables=total_tables)
    
    minio_results = []  # Store MinIO results for Phase 2
    
    for i, table_config in enumerate(run_config["tables"], 1):
        table_name = table_config.get('source_table')
        
        logger.info(f"Extracting table {i}/{total_tables} to MinIO",
                   table=table_name,
                   progress=f"{i}/{total_tables}",
                   phase="extraction_phase")
        
        with BatchOperation(logger, "extraction_to_minio", table=table_name) as extraction_op:
            # Create table-specific run config
            table_run_config = {
                **run_config,
                "current_table": table_config
            }
            
            # Extract data from MySQL
            data = extract_data_from_mysql(table_run_config)
            
            # Load to MinIO
            minio_result = load_data_to_minio(data)
            
            # Store results for Snowflake loading phase
            minio_results.append({
                "table_config": table_config,
                "data": data,
                "minio_file_info": minio_result
            })
            
            # Update operation metrics
            extraction_op.records_processed = minio_result.get("rows_loaded", 0)
            
            logger.info(f"Table extraction to MinIO completed",
                       table=table_name,
                       rows_processed=extraction_op.records_processed,
                       progress=f"{i}/{total_tables}",
                       phase="extraction_phase")
    
    logger.info("✅ PHASE 1 COMPLETED: All data extracted to MinIO", 
               phase="extraction_phase",
               tables_extracted=len(minio_results),
               total_records=sum(result["minio_file_info"].get("rows_loaded", 0) for result in minio_results))
    
    # PHASE 2: Load all data to Snowflake with ingestion_date
    logger.info("🔄 PHASE 2: Starting data loading to Snowflake", 
               phase="snowflake_loading_phase",
               total_tables=len(minio_results),
               ingestion_date=ingestion_date)
    
    for i, result in enumerate(minio_results, 1):
        table_config = result["table_config"]
        table_name = table_config.get('source_table')
        
        logger.info(f"Loading table {i}/{len(minio_results)} to Snowflake",
                   table=table_name,
                   progress=f"{i}/{len(minio_results)}",
                   phase="snowflake_loading_phase")
        
        with BatchOperation(logger, "loading_to_snowflake", table=table_name) as loading_op:
            # Prepare data with MinIO info and ingestion_date
            data_with_minio_info = {
                **result["data"],
                "minio_file_info": result["minio_file_info"],
                "ingestion_date": ingestion_date,  # Add ingestion date for Snowflake
                "add_ingestion_column": True  # Flag to indicate we want to add/update ingestion_date column
            }
            
            # Load to Snowflake with ingestion_date handling
            snowflake_result = load_data_to_snowflake(data_with_minio_info)
            
            # Update operation metrics
            loading_op.records_processed = result["minio_file_info"].get("rows_loaded", 0)
            
            logger.info(f"Table loading to Snowflake completed",
                       table=table_name,
                       rows_processed=loading_op.records_processed,
                       progress=f"{i}/{len(minio_results)}",
                       phase="snowflake_loading_phase",
                       ingestion_date=ingestion_date)
    
    logger.info("✅ PHASE 2 COMPLETED: All data loaded to Snowflake", 
               phase="snowflake_loading_phase",
               tables_loaded=len(minio_results),
               ingestion_date=ingestion_date)
    
    # Log final summary
    total_records_processed = sum(result["minio_file_info"].get("rows_loaded", 0) for result in minio_results)
    summary = logger.log_batch_summary()
    logger.info("🎉 Batch ETL pipeline completed successfully",
               total_tables_processed=len(minio_results),
               total_records=total_records_processed,
               success_rate=summary.get("success_rate", 0),
               ingestion_date=ingestion_date,
               phase="pipeline_complete")