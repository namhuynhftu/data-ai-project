"""
ETL Pipeline: MySQL → MinIO → Snowflake
"""
import json 
from datetime import datetime

from elt_pipeline.batch.ops.load_data_to_snowflake import load_minio_to_snowflake_via_stage_direct
from elt_pipeline.batch.ops.extract_data_from_mysql import load_run_config, extract_data_from_mysql
from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
from elt_pipeline.logger_utils import BatchLogger, BatchOperation

metadata_path = "elt_pipeline/batch/pipelines/metadata/table_metadata.json"

# Pipeline Configuration
PIPELINE_CONFIG = {
    "add_ingestion_date": True,
    "log_detailed_metrics": True
}

if __name__ == "__main__":
    logger = BatchLogger("batch_pipeline")
    start_time = datetime.now()
    
    logger.info("Starting ETL Pipeline", workflow="MySQL → MinIO → Snowflake")
    
    # Load configuration
    run_config = load_run_config(metadata_path)
    total_tables = len(run_config["tables"])
    ingestion_date = datetime.now().isoformat()
    
    logger.info("Pipeline loaded", 
               tables=total_tables,
               ingestion_date=ingestion_date)
    
    # PHASE 1: Extract to MinIO
    logger.info("PHASE 1: Extracting to MinIO")
    minio_results = []
    
    for i, table_config in enumerate(run_config["tables"], 1):
        table_name = table_config.get('source_table')
        
        logger.info(f"Extracting {i}/{total_tables}", table=table_name)
        
        with BatchOperation(logger, "extraction_to_minio", table=table_name) as extraction_op:
            # Create table-specific run config
            table_run_config = {**run_config, "current_table": table_config}
            
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
            
            extraction_op.records_processed = minio_result.get("rows_loaded", 0)
            
            logger.info("Extracted to MinIO", 
                       table=table_name,
                       rows=extraction_op.records_processed)
    
    total_records = sum(result["minio_file_info"].get("rows_loaded", 0) for result in minio_results)
    logger.info("PHASE 1 COMPLETED", 
               tables=len(minio_results),
               total_records=f"{total_records:,}")
    
    # PHASE 2: Load to Snowflake
    logger.info("PHASE 2: Loading to Snowflake")
    
    for i, result in enumerate(minio_results, 1):
        table_config = result["table_config"]
        table_name = table_config.get('source_table')
        
        logger.info(f"Loading {i}/{len(minio_results)} to Snowflake", table=table_name)
        
        with BatchOperation(logger, "loading_to_snowflake", table=table_name) as loading_op:
            # Prepare data with MinIO info and ingestion_date
            data_with_minio_info = {
                **result["data"],
                "minio_file_info": result["minio_file_info"],
                "ingestion_date": ingestion_date,
                "add_ingestion_column": PIPELINE_CONFIG["add_ingestion_date"]
            }
            
            # Load to Snowflake (with temp file fallback for localhost)
            snowflake_result = load_minio_to_snowflake_via_stage_direct(data_with_minio_info)
            
            # Update metrics
            rows_processed = snowflake_result.get("total_rows_loaded", 0)
            loading_op.records_processed = rows_processed
            
            logger.info("Loaded to Snowflake", 
                       table=table_name,
                       rows=rows_processed,
                       method=snowflake_result.get("method", "temp_file"))
    
    logger.info("PHASE 2 COMPLETED", 
               tables_loaded=len(minio_results))
    
    # Final summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    total_records_processed = sum(result["minio_file_info"].get("rows_loaded", 0) for result in minio_results)
    summary = logger.log_batch_summary()
    
    logger.info("=" * 60)
    logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    logger.info(f"  Tables Processed: {len(minio_results)}")
    logger.info(f"  Total Records: {total_records_processed:,}")
    logger.info(f"  Duration: {duration:.1f} seconds")
    logger.info(f"  Success Rate: {summary.get('success_rate', 100):.1f}%")
    logger.info("=" * 60)