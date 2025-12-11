"""
ETL Pipeline: MySQL → MinIO → Snowflake
"""
import json 
import sys
from datetime import datetime

from elt_pipeline.batch.ops.load_data_to_snowflake import load_minio_to_snowflake_via_stage_direct
from elt_pipeline.batch.ops.extract_data_from_mysql import load_run_config, extract_data_from_mysql
from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
from elt_pipeline.batch.utils.loaded_at_tracker import update_table_loaded_at
from elt_pipeline.logger_utils import BatchLogger, BatchOperation

metadata_path = "elt_pipeline/batch/pipelines/metadata/table_metadata.json"

# Pipeline Configuration
PIPELINE_CONFIG = {
    "add_ingestion_date": True,
    "log_detailed_metrics": True
}

def get_strategy_filter():
    """
    Prompt user to select which tables to process based on load strategy.
    Returns: 'all', 'all_full_load', 'full_load', or 'incremental_by_watermark'
    """
    print("\n" + "=" * 60)
    print("ETL PIPELINE - TABLE STRATEGY SELECTION")
    print("=" * 60)
    print("Select which tables to process:")
    print("  1. All tables with their default strategies (full_load and incremental)")
    print("  2. All tables with FULL_LOAD strategy (override incremental)")
    print("  3. Only full_load tables")
    print("  4. Only incremental_by_watermark tables")
    print("=" * 60)
    
    while True:
        choice = input("Enter your choice (1-4): ").strip()
        
        if choice == "1":
            print("Selected: All tables with default strategies\n")
            return "all"
        elif choice == "2":
            print("Selected: All tables with FULL_LOAD strategy\n")
            return "all_full_load"
        elif choice == "3":
            print("Selected: Only full_load tables\n")
            return "full_load"
        elif choice == "4":
            print("Selected: Only incremental_by_watermark tables\n")
            return "incremental_by_watermark"
        else:
            print("Invalid choice. Please enter 1, 2, 3, or 4.\n")

def filter_tables_by_strategy(tables, strategy_filter):
    """
    Filter tables based on the selected strategy.
    For 'all_full_load', returns all tables but overrides their strategy to 'full_load'.
    """
    if strategy_filter == "all":
        return tables
    
    if strategy_filter == "all_full_load":
        # Override all tables to use full_load strategy
        overridden_tables = []
        for table in tables:
            table_copy = table.copy()
            original_strategy = table_copy.get("strategy", "full_load")
            table_copy["strategy"] = "full_load"
            table_copy["original_strategy"] = original_strategy  # Keep track of original
            overridden_tables.append(table_copy)
        return overridden_tables
    
    filtered = [table for table in tables if table.get("strategy") == strategy_filter]
    return filtered

if __name__ == "__main__":
    logger = BatchLogger("batch_pipeline")
    start_time = datetime.now()
    
    logger.info("Starting ETL Pipeline", workflow="MySQL → MinIO → Snowflake")
    
    # Get strategy filter from user
    strategy_filter = get_strategy_filter()
    
    # Load configuration
    run_config = load_run_config(metadata_path)
    
    # Filter tables based on strategy
    all_tables = run_config["tables"]
    filtered_tables = filter_tables_by_strategy(all_tables, strategy_filter)
    
    if not filtered_tables:
        logger.warning("No tables match the selected strategy", strategy=strategy_filter)
        print(f"\n⚠ No tables found with strategy: {strategy_filter}")
        sys.exit(0)
    
    # Update run_config with filtered tables
    run_config["tables"] = filtered_tables
    
    total_tables = len(filtered_tables)
    ingestion_date = datetime.now().isoformat()
    
    # Log strategy override if applicable
    if strategy_filter == "all_full_load":
        logger.info("Pipeline loaded with FULL_LOAD override for all tables", 
                   tables=total_tables,
                   strategy_filter=strategy_filter,
                   ingestion_date=ingestion_date)
    else:
        logger.info("Pipeline loaded", 
                   tables=total_tables,
                   strategy_filter=strategy_filter,
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
            
            # Extract data from MySQL (includes data masking/hashing based on schema contract)
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
            
            logger.info("Extracted to MinIO with data masking applied", 
                       table=table_name,
                       rows=extraction_op.records_processed,
                       masked_columns_applied=True)
    
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
                "add_ingestion_column": PIPELINE_CONFIG["add_ingestion_date"],
                "is_first_incremental_load": result["data"].get("is_first_incremental_load", False)
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
            
            # Update loaded_at.json for incremental tables after successful load
            # Skip update if strategy was overridden to full_load
            original_strategy = table_config.get("original_strategy")
            current_strategy = table_config.get("strategy")
            
            if current_strategy == "incremental_by_watermark" and strategy_filter != "all_full_load":
                new_watermark = result["data"].get("new_watermark")
                if new_watermark:
                    update_table_loaded_at(table_name, new_watermark)
                    logger.info("Updated loaded_at metadata", 
                               table=table_name,
                               new_watermark=new_watermark)
                else:
                    # If no new watermark, update with current timestamp
                    update_table_loaded_at(table_name)
                    logger.info("Updated loaded_at metadata with current timestamp", 
                               table=table_name)
            elif original_strategy == "incremental_by_watermark" and strategy_filter == "all_full_load":
                logger.info("Skipped loaded_at update (strategy overridden to full_load)", 
                           table=table_name,
                           original_strategy=original_strategy)
    
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
    logger.info(f"  Loading Strategy: {strategy_filter}")
    logger.info(f"  Tables Processed: {len(minio_results)}")
    logger.info(f"  Total Records: {total_records_processed:,}")
    logger.info(f"  Duration: {duration:.1f} seconds")
    logger.info(f"  Success Rate: {summary.get('success_rate', 100):.1f}%")
    logger.info("=" * 60)