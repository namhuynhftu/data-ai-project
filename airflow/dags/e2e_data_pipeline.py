import base64
import json
import os
import sys
from pathlib import Path

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import pendulum


def get_mysql_config(mysql_conn):
    """Get MySQL configuration from Airflow connection."""
    return {
        "host": mysql_conn.host,
        "port": mysql_conn.port or 3306,
        "user": mysql_conn.login,
        "password": mysql_conn.password,
        "database": mysql_conn.schema
    }


def get_minio_config(minio_conn):
    """Get MinIO configuration from Airflow connection."""
    extra = json.loads(minio_conn.extra) if minio_conn.extra else {}
    endpoint_url = extra.get("endpoint_url", "http://minio:9000")
    
    return {
        "endpoint_url": endpoint_url,
        "endpoint": endpoint_url.replace("http://", "").replace("https://", ""),
        "access_key": minio_conn.login or "minioadmin",
        "secret_key": minio_conn.password or "minioadmin",
        "region_name": extra.get("region_name", "us-east-1"),
        "bucket_name": "data-lake"
    }


def get_snowflake_config(snow_conn):
    """Get Snowflake configuration from Airflow connection."""
    extra = json.loads(snow_conn.extra) if snow_conn.extra else {}
    private_key_b64 = extra.get("private_key_content", "")
    
    # Decode private key
    private_key = base64.b64decode(private_key_b64).decode("utf-8") if private_key_b64 else ""
    
    # Write private key to temp file
    import tempfile
    key_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
    key_file.write(private_key)
    key_file.close()
    
    return {
        "account": extra.get("account"),
        "user": snow_conn.login,
        "private_key_file": key_file.name,
        "private_key_file_pwd": snow_conn.password,
        "warehouse": extra.get("warehouse"),
        "database": extra.get("database"),
        "schema": snow_conn.schema or "RAW_DATA",
        "role": extra.get("role"),
        "authenticator": "SNOWFLAKE_JWT"
    }


@dag(
    dag_id="e2e_batch_elt_pipeline",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["elt", "batch", "mysql", "minio", "snowflake", "dbt", "data-masking"],
    max_active_runs=1,
    description="End-to-end ELT pipeline from MySQL to Snowflake with dbt transformation and data masking",
)
def e2e_batch_elt_pipeline():
    """
    ### E2E Batch ELT Pipeline with Data Masking
    """

    @task
    def validate_connections():
        """Validate MySQL, MinIO, and Snowflake connections before processing."""
        from airflow.hooks.base import BaseHook
        import mysql.connector
        from minio import Minio
        import snowflake.connector
        
        validation_results = {}
        
        # Get connections
        mysql_conn = BaseHook.get_connection("mysql_conn")
        minio_conn = BaseHook.get_connection("minio_conn")
        snow_conn = BaseHook.get_connection("snowflake_conn")
        
        # Get configurations using helper functions
        mysql_config = get_mysql_config(mysql_conn)
        minio_config = get_minio_config(minio_conn)
        
        # Validate MySQL connection
        try:
            connection = mysql.connector.connect(
                host=mysql_config["host"],
                port=mysql_config["port"],
                user=mysql_config["user"],
                password=mysql_config["password"],
                database=mysql_config["database"],
                auth_plugin='mysql_native_password'
            )
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                connection.close()
                validation_results['mysql'] = {'status': 'success', 'message': 'Connection validated'}
            else:
                raise Exception("Connection not established")
        except Exception as e:
            validation_results['mysql'] = {'status': 'failed', 'message': str(e)}
            raise AirflowException(f"MySQL connection validation failed: {e}")
        
        # Validate MinIO connection
        try:
            client = Minio(
                endpoint=minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=False
            )
            # Test by listing buckets
            list(client.list_buckets())
            validation_results['minio'] = {'status': 'success', 'message': 'Connection validated'}
        except Exception as e:
            validation_results['minio'] = {'status': 'failed', 'message': str(e)}
            raise AirflowException(f"MinIO connection validation failed: {e}")
        
        # Validate Snowflake connection
        try:
            extra = json.loads(snow_conn.extra) if snow_conn.extra else {}
            private_key_b64 = extra.get("private_key_content", "")
            
            # Write private key to temp file for connection
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as key_file:
                private_key = base64.b64decode(private_key_b64).decode("utf-8")
                key_file.write(private_key)
                key_file_path = key_file.name
            
            try:
                conn = snowflake.connector.connect(
                    account=extra.get("account"),
                    user=snow_conn.login,
                    private_key_file=key_file_path,
                    private_key_file_pwd=snow_conn.password,
                    warehouse=extra.get("warehouse"),
                    database=extra.get("database"),
                    schema=snow_conn.schema,
                    role=extra.get("role"),
                    authenticator='SNOWFLAKE_JWT'
                )
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                cursor.fetchone()
                cursor.close()
                conn.close()
                validation_results['snowflake'] = {'status': 'success', 'message': 'Connection validated'}
            finally:
                os.unlink(key_file_path)
                
        except Exception as e:
            validation_results['snowflake'] = {'status': 'failed', 'message': str(e)}
            raise AirflowException(f"Snowflake connection validation failed: {e}")
        
        print("All connections validated successfully!")
        return validation_results

    @task
    def extract_and_load_pipeline(validation_results: dict):
        """
        Combined ETL pipeline: Extract from MySQL -> Load to MinIO -> Load to Snowflake.
        Uses single connections for each system to optimize performance.
        """
        from airflow.hooks.base import BaseHook
        
        # Add batch module to Python path
        project_root = Path("/opt/airflow")
        sys.path.insert(0, str(project_root))
        
        from elt_pipeline.batch.ops.extract_data_from_mysql import extract_data_from_mysql, load_run_config
        from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
        from elt_pipeline.batch.ops.load_data_to_snowflake import load_minio_to_snowflake_via_stage_direct
        
        # Get all connections once
        mysql_conn = BaseHook.get_connection("mysql_conn")
        minio_conn = BaseHook.get_connection("minio_conn")
        snow_conn = BaseHook.get_connection("snowflake_conn")
        
        # Get configurations
        mysql_config = get_mysql_config(mysql_conn)
        minio_config = get_minio_config(minio_conn)
        snowflake_config = get_snowflake_config(snow_conn)
        
        # Load table metadata
        config_path = project_root / "elt_pipeline" / "batch" / "pipelines" / "metadata" / "table_metadata.json"
        run_config = load_run_config(str(config_path))
        
        # Configure all connections in run_config
        run_config["data_source_config"] = mysql_config
        run_config["minio_target_storage"] = {
            "endpoint": minio_config["endpoint"],
            "access_key": minio_config["access_key"],
            "secret_key": minio_config["secret_key"],
            "bucket": minio_config["bucket_name"],
            "default_format": "parquet",
            "default_compression": "snappy",
            "secure": False
        }
        run_config["snowflake_target_storage"] = snowflake_config
        
        # Process each table through the entire pipeline
        pipeline_results = []
        
        print("=" * 70)
        print("Starting E2E Data Pipeline")
        print("=" * 70)
        
        for idx, table_config in enumerate(run_config["tables"], 1):
            table_name = table_config["source_table"]
            
            print(f"\n[{idx}/{len(run_config['tables'])}] Processing: {table_name}")
            print("-" * 70)
            
            run_config["current_table"] = table_config
            
            # Step 1: Extract from MySQL with data masking
            print(f"  Extracting from MySQL (with masking)...")
            extracted_data = extract_data_from_mysql(run_config)
            row_count = len(extracted_data["data"]) if extracted_data["data"] is not None else 0
            print(f"  Extracted {row_count:,} rows")
            
            # Step 2: Load to MinIO
            print(f"  Loading to MinIO data lake...")
            minio_result = load_data_to_minio(extracted_data)
            print(f"  Loaded to {minio_result['bucket']}/{minio_result['file_name']}")
            
            # Step 3: Load to Snowflake via internal stage
            print(f"  Loading to Snowflake...")
            extracted_data["minio_file_info"] = minio_result
            snowflake_result = load_minio_to_snowflake_via_stage_direct(extracted_data)
            print(f"  Loaded {snowflake_result['total_rows_loaded']:,} rows to Snowflake")
            
            pipeline_results.append({
                "table": table_name,
                "extracted_rows": row_count,
                "minio_file": minio_result["file_name"],
                "snowflake_rows": snowflake_result["total_rows_loaded"],
                "stage": snowflake_result["stage_name"],
                "method": snowflake_result["method"]
            })
        
        print("\n" + "=" * 70)
        print(f"Pipeline completed for {len(pipeline_results)} tables")
        print("=" * 70)
        
        return pipeline_results
    

    @task
    def cleanup_and_validate_data(pipeline_results: list):
        """
        Combined task to clean up internal stage and validate data in Snowflake.
        Uses a single connection for both operations to optimize performance.
        """
        from airflow.hooks.base import BaseHook
        import snowflake.connector
        
        # Get Snowflake connection
        snow_conn = BaseHook.get_connection("snowflake_conn")
        snowflake_config = get_snowflake_config(snow_conn)
        
        # Load table metadata to get list of tables
        project_root = Path("/opt/airflow")
        config_path = project_root / "elt_pipeline" / "batch" / "pipelines" / "metadata" / "table_metadata.json"
        
        import json
        with open(config_path, "r") as f:
            metadata = json.load(f)
        
        tables = metadata.get("tables", [])
        
        try:
            # Connect to Snowflake once for both cleanup and validation
            conn = snowflake.connector.connect(
                account=snowflake_config["account"],
                user=snowflake_config["user"],
                private_key_file=snowflake_config["private_key_file"],
                private_key_file_pwd=snowflake_config["private_key_file_pwd"],
                warehouse=snowflake_config["warehouse"],
                database=snowflake_config["database"],
                schema=snowflake_config["schema"],
                role=snowflake_config["role"],
                authenticator='SNOWFLAKE_JWT'
            )
            cursor = conn.cursor()
            
            # ========== STEP 1: Clean up internal stage ==========
            print("\n" + "=" * 70)
            print("STEP 1: Cleaning up internal stage files")
            print("=" * 70)
            
            cleanup_results = []
            stage_name = pipeline_results[0]["stage"] if pipeline_results else "MINIO_STAGE_SHARED"
            
            for result in pipeline_results:
                table_name = result["table"].lower()
                
                try:
                    remove_query = f"REMOVE @{snowflake_config['database']}.{snowflake_config['schema']}.{stage_name}/{table_name}/"
                    cursor.execute(remove_query)
                    removed = cursor.fetchall()
                    files_removed = len(removed)
                    
                    cleanup_results.append({
                        "table": table_name,
                        "files_removed": files_removed,
                        "status": "cleaned"
                    })
                    
                    print(f"Cleaned {files_removed} file(s) from stage: {table_name}")
                except Exception as e:
                    print(f"  ⚠ Warning: Failed to clean stage for {table_name}: {e}")
                    cleanup_results.append({
                        "table": table_name,
                        "files_removed": 0,
                        "status": "failed",
                        "error": str(e)
                    })
            
            print(f"\nStage cleanup completed for {len(cleanup_results)} tables")
            
            # ========== STEP 2: Validate data in tables ==========
            print("\n" + "=" * 70)
            print("STEP 2: Validating data in Snowflake tables")
            print("=" * 70)
            
            validation_results = []
            failed_tables = []
            
            for table_config in tables:
                table_name = table_config["targets"]["snowflake"]["target_table"].upper()
                
                # Check row count using COUNT(1)
                query = f"SELECT COUNT(1) FROM {snowflake_config['database']}.{snowflake_config['schema']}.{table_name}"
                cursor.execute(query)
                row_count = cursor.fetchone()[0]
                
                validation_result = {
                    "table": table_name,
                    "row_count": row_count,
                    "status": "passed" if row_count > 0 else "failed"
                }
                validation_results.append(validation_result)
                
                if row_count == 0:
                    failed_tables.append(table_name)
                
                status_icon = "✓" if row_count > 0 else "✗"
                status_text = "PASSED" if row_count > 0 else "FAILED"
                print(f"  {status_icon} {table_name}: {row_count:,} rows - {status_text}")
            
            cursor.close()
            conn.close()
            
            print("\n" + "=" * 70)
            
            # Fail pipeline if any table has 0 rows
            if failed_tables:
                error_msg = f"Data validation failed: {', '.join(failed_tables)} have 0 rows"
                print(f"✗ {error_msg}")
                raise AirflowException(error_msg)
            
            print(f"All {len(validation_results)} tables passed validation")
            print("=" * 70 + "\n")
            
            return {
                "cleanup": cleanup_results,
                "validation": validation_results
            }
            
        except snowflake.connector.Error as e:
            raise AirflowException(f"Snowflake operation error: {e}")
        finally:
            # Clean up temp private key file
            if os.path.exists(snowflake_config["private_key_file"]):
                try:
                    os.unlink(snowflake_config["private_key_file"])
                except Exception:
                    pass

    @task
    def dbt_build(validation_results: list):
        """
        Transform data in Snowflake using dbt.
        Environment variables are set from Airflow Snowflake connection.
        """
        from airflow.hooks.base import BaseHook
        import subprocess
        
        # Get Snowflake connection
        snow_conn = BaseHook.get_connection("snowflake_conn")
        snowflake_config = get_snowflake_config(snow_conn)
        
        # Set environment variables for dbt
        env = os.environ.copy()
        env.update({
            "SNOWFLAKE_ACCOUNT": snowflake_config["account"],
            "SNOWFLAKE_USER": snowflake_config["user"],
            "SNOWFLAKE_PRIVATE_KEY_FILE_PATH": snowflake_config["private_key_file"],
            "SNOWFLAKE_PRIVATE_KEY_FILE_PWD": snowflake_config["private_key_file_pwd"],
            "SNOWFLAKE_DATABASE": snowflake_config["database"],
            "SNOWFLAKE_WAREHOUSE": snowflake_config["warehouse"],
            "SNOWFLAKE_ROLE": snowflake_config["role"],
            "SNOWFLAKE_SCHEMA": snowflake_config["schema"],
            "DBT_PROFILES_DIR": "/opt/airflow/.dbt"
        })
        
        # Run dbt build
        result = subprocess.run(
            ["dbt", "build", "--profiles-dir", "/opt/airflow/.dbt", "--target", "dev"],
            cwd="/opt/airflow/dwh/snowflake",
            env=env,
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        
        if result.returncode != 0:
            raise Exception(f"dbt build failed with return code {result.returncode}")
        
        return {"success": True, "message": "dbt build completed"}

    # Define task dependencies
    validation = validate_connections()
    etl_pipeline = extract_and_load_pipeline(validation)
    cleanup_validation = cleanup_and_validate_data(etl_pipeline)
    transformation = dbt_build(cleanup_validation)

    validation >> etl_pipeline >> cleanup_validation >> transformation


# Create the DAG instance
e2e_batch_elt_pipeline()