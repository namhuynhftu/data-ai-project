from pathlib import Path
import os
from datetime import datetime
import logging
from typing import Optional, Dict, Any

from elt_pipeline.utils.mysql_loader import MySQLLoader
from elt_pipeline.utils.psql_loader import PSQLLoader
from elt_pipeline.utils.minio_loader import MinIOLoader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Factory Method
def get_data_loader(db_type: str, params: dict):
    """Factory method to create appropriate data loader."""
    loaders = {
        "mysql": MySQLLoader,
        "postgresql": PSQLLoader,
        "minio": MinIOLoader
    }
    
    loader_class = loaders.get(db_type.lower())
    if not loader_class:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    return loader_class(params)

def get_all_tables(mysql_params: Dict[str, Any], schema_name: str = None) -> list:
    """Get all table names from MySQL database."""
    try:
        logger.info("Retrieving all table names from MySQL")
        
        with get_data_loader("mysql", mysql_params) as db_loader:
            if schema_name:
                sql = f"SHOW TABLES FROM {schema_name}"
            else:
                sql = "SHOW TABLES"
            
            tables_df = db_loader.extract_data(sql)
            # The column name varies, so get the first column
            table_names = tables_df.iloc[:, 0].tolist()
            
        logger.info(f"Found {len(table_names)} tables: {table_names}")
        return table_names
        
    except Exception as e:
        logger.error(f"Failed to retrieve table names: {e}")
        raise

def extract_data_from_mysql(context: Optional[Any] = None, run_config: Optional[Dict[str, Any]] = None):
    """Extract data from MySQL Docker container."""
    
    # Use logger if context is not available
    log = context.log if context and hasattr(context, 'log') else logger
    
    updated_at = run_config.get("updated_at")
    if updated_at is None or updated_at == "":
        log.info("No updated_at provided, extracting all data.")
        return None
    log.info(f"Extracting data updated after {updated_at}")

    # Choose extract strategy
    sql_stm = f"""
        SELECT * FROM {run_config.get('source_tbl')}
        WHERE 1=1
    """

    if run_config.get("strategy") == "incremental_by_partition":
        if updated_at != "init_dump":
            sql_stm += f""" AND CAST({run_config.get('partition')} AS DATE)= '{updated_at}'"""

    elif run_config.get("strategy") == "incremental_by_watermark":
        data_loader = get_data_loader(run_config.get("db_provider"), run_config.get("target_db_params"))

        watermark = data_loader.get_watermark(
            f"{run_config.get('target_schema')}.{run_config.get('target_tbl')}",
            run_config.get("watermark")
        )

        watermark = updated_at if watermark is None or watermark > updated_at else watermark
        if updated_at != "init_dump":
            sql_stm += f""" AND {run_config.get('watermark')} >= '{watermark}'"""
        
    log.info(f"Extracting data with SQL: {sql_stm}")
    
    # Use context manager for proper resource management
    with get_data_loader("mysql", run_config.get("source_db_params")) as db_loader:
        pd_data = db_loader.extract_data(sql_stm)
        log.info(f"Extracted {len(pd_data)} rows from MySQL")

        # Update params for downstream tasks
        run_config.update({
            "updated_at": updated_at, 
            "data": pd_data,
            "path": f"bronze/{run_config.get('data_source')}/{run_config.get('domain_name')}/{run_config.get('source_tbl')}"
        })
        return pd_data

def load_data_to_minio(data_frame, context: Optional[Any] = None, run_config: Optional[Dict[str, Any]] = None) -> str:
    """Load extracted data to MinIO in Bronze layer following medallion architecture."""
    
    # Use logger if context is not available
    log = context.log if context and hasattr(context, 'log') else logger
    
    if data_frame is None or len(data_frame) == 0:
        log.info("No data extracted from MySQL, skipping load to MinIO.")
        return None
    
    try:
        # Construct the Bronze layer path following best practices
        # Format: bronze/data_source/schema_name/table_name/year=YYYY/month=MM/day=DD/
        timestamp = datetime.now()
        schema_name = run_config.get('source_schema', 'default_schema')
        table_name = run_config.get('source_tbl')
        
        # Create partitioned path for better organization
        object_path = (
            f"bronze/mysql/{schema_name}/{table_name}/"
            f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/"
            f"{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        
        log.info(f"Loading {len(data_frame)} rows to MinIO at path: {object_path}")
        
        # Get MinIO configuration from environment
        minio_params = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ROOT_USER', 'user'),
            'secret_key': os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            'secure': False  # Set to True for HTTPS in production
        }
        
        # Load data to MinIO using context manager
        with get_data_loader("minio", minio_params) as minio_loader:
            load_params = {
                'bucket_name': 'bronze',  # Bronze layer bucket
                'object_path': object_path
            }
            
            rows_loaded = minio_loader.load_data(data_frame, load_params)
            log.info(f"Successfully loaded {rows_loaded} rows to MinIO")
            
            return f"s3://bronze/{object_path}"
    
    except Exception as e:
        log.error(f"Failed to load data to MinIO: {e}")
        raise

def extract_and_load_all_tables(run_config: Dict[str, Any]) -> Dict[str, str]:
    """Extract all tables from MySQL and load to MinIO."""
    try:
        logger.info("Starting MySQL to MinIO ETL pipeline for all tables")
        
        # Get all table names
        table_names = get_all_tables(
            run_config.get("source_db_params"), 
            run_config.get("source_schema")
        )
        
        results = {}
        successful_tables = 0
        failed_tables = 0
        
        for table_name in table_names:
            try:
                logger.info(f"Processing table: {table_name}")
                
                # Create config for this specific table
                table_config = run_config.copy()
                table_config.update({
                    "source_tbl": table_name,
                    "updated_at": datetime.now().strftime("%Y-%m-%d")
                })
                
                # Step 1: Extract data from MySQL
                data_frame = extract_data_from_mysql(run_config=table_config)
                
                if data_frame is not None and len(data_frame) > 0:
                    # Step 2: Load data to MinIO
                    s3_path = load_data_to_minio(data_frame, run_config=table_config)
                    results[table_name] = s3_path
                    successful_tables += 1
                    logger.info(f"✅ Successfully processed {table_name}: {len(data_frame)} rows")
                else:
                    results[table_name] = "No data found"
                    logger.warning(f"⚠️ No data found in table: {table_name}")
                    
            except Exception as e:
                failed_tables += 1
                error_msg = f"Failed to process table {table_name}: {e}"
                results[table_name] = error_msg
                logger.error(f"❌ {error_msg}")
                continue  # Continue with next table
        
        logger.info(f"Pipeline completed! ✅ {successful_tables} successful, ❌ {failed_tables} failed")
        return results
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

def extract_and_load_pipeline(run_config: Dict[str, Any]) -> str:
    """Complete pipeline to extract from MySQL and load to MinIO."""
    try:
        logger.info("Starting MySQL to MinIO ETL pipeline")
        
        # Step 1: Extract data from MySQL
        data_frame = extract_data_from_mysql(run_config=run_config)
        
        # Step 2: Load data to MinIO
        s3_path = load_data_to_minio(data_frame, run_config=run_config)
        
        logger.info(f"Pipeline completed successfully. Data available at: {s3_path}")
        return s3_path
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Configuration for all tables
    all_tables_config = {
        "source_db_params": {
            "host": os.getenv("MYSQL_HOST", "localhost"),
            "port": int(os.getenv("MYSQL_PORT", 3306)),
            "user": os.getenv("MYSQL_USER", "user"),
            "password": os.getenv("MYSQL_PASSWORD", "password"),
            "database": os.getenv("MYSQL_DATABASE", "ecommerce_db")
        },
        "source_schema": "ecommerce_db",
        "strategy": "full_load",  # or "incremental_by_watermark", "incremental_by_partition"
        "data_source": "mysql",
        "domain_name": "ecommerce"
    }
    
    print("🚀 Starting extraction of ALL tables from MySQL to MinIO...")
    print("=" * 60)
    
    try:
        # Load all tables
        results = extract_and_load_all_tables(all_tables_config)
        
        print("\n📊 FINAL RESULTS:")
        print("=" * 60)
        
        successful = 0
        failed = 0
        
        for table_name, result in results.items():
            if result.startswith("s3://"):
                print(f"✅ {table_name:<30} → {result}")
                successful += 1
            else:
                print(f"❌ {table_name:<30} → {result}")
                failed += 1
        
        print("=" * 60)
        print(f"📈 SUMMARY: {successful} successful, {failed} failed")
        print("🎉 All tables processing completed!")
        
    except Exception as e:
        print(f"💥 Pipeline execution failed: {e}")