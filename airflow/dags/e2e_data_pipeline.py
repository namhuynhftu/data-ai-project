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


def get_dbt_snowflake_env_vars():
    """
    Get dbt Snowflake environment variables from Airflow connections.x
    """
    # Get the Snowflake connection
    conn = BaseHook.get_connection("snowflake_default")
    extra = json.loads(conn.extra) if conn.extra else {}

    # Decode the base64-encoded private key
    private_key_b64 = extra.get("private_key_content", "")
    private_key = base64.b64decode(private_key_b64).decode("utf-8") if private_key_b64 else ""

    return {
        "SNOWFLAKE_ACCOUNT": extra.get("account", ""),
        "SNOWFLAKE_USER": conn.login or "",
        "SNOWFLAKE_PRIVATE_KEY_FILE_PWD": conn.password or "",
        "SNOWFLAKE_ROLE": extra.get("role", ""),
        "SNOWFLAKE_WAREHOUSE": extra.get("warehouse", ""),
        "SNOWFLAKE_DATABASE": extra.get("database", ""),
        "SNOWFLAKE_SCHEMA": conn.schema or "",
        "SNOWFLAKE_PRIVATE_KEY": private_key,
        "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
        "DBT_PROJECT_DIR": "/opt/airflow/dwh",
        "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", ""),
    }

@dag(
    dag_id="e2e_batch_elt_pipeline",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["elt", "batch", "mysql", "minio", "snowflake", "dbt"],
    max_active_runs=1,
    description="End-to-end ELT pipeline from MySQL to Snowflake with dbt transformation",
)
def e2e_batch_elt_pipeline():
    """
    ### E2E Batch ELT Pipeline
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
        
        print("✅ All connections validated successfully!")
        return validation_results

    @task
    def extract_from_mysql(validation_results: dict):
        """Extract data from MySQL database."""
        from airflow.hooks.base import BaseHook
        
        # Add batch module to Python path
        project_root = Path("/opt/airflow")
        sys.path.insert(0, str(project_root))
        
        from elt_pipeline.batch.ops.extract_data_from_mysql import extract_data_from_mysql, load_run_config
        
        # Get MySQL connection and config
        mysql_conn = BaseHook.get_connection("mysql_conn")
        mysql_config = get_mysql_config(mysql_conn)
        
        # Load configuration
        config_path = project_root / "elt_pipeline" / "batch" / "pipelines" / "metadata" / "table_metadata.json"
        run_config = load_run_config(str(config_path))
        
        # Override MySQL connection details from Airflow connection
        run_config["data_source_config"] = mysql_config
        
        extraction_results = []
        for table_config in run_config["tables"]:
            run_config["current_table"] = table_config
            result = extract_data_from_mysql(run_config)
            extraction_results.append({
                "table": table_config["source_table"],
                "records": len(result["data"]) if result["data"] is not None else 0
            })
        
        print(f"✅ Extracted data from {len(extraction_results)} tables")
        return extraction_results

    @task
    def load_to_minio(extraction_results: list):
        """Load extracted data to MinIO (S3-compatible storage)."""
        from airflow.hooks.base import BaseHook
        
        project_root = Path("/opt/airflow")
        sys.path.insert(0, str(project_root))
        
        from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
        from elt_pipeline.batch.ops.extract_data_from_mysql import load_run_config, extract_data_from_mysql
        
        # Get connections and configs
        mysql_conn = BaseHook.get_connection("mysql_conn")
        minio_conn = BaseHook.get_connection("minio_conn")
        
        mysql_config = get_mysql_config(mysql_conn)
        minio_config = get_minio_config(minio_conn)
        
        config_path = project_root / "elt_pipeline" / "batch" / "pipelines" / "metadata" / "table_metadata.json"
        run_config = load_run_config(str(config_path))
        
        # Override connection details from Airflow connections
        run_config["data_source_config"] = mysql_config
        
        # Configure MinIO target storage
        run_config["minio_target_storage"] = {
            "endpoint": minio_config["endpoint"],
            "access_key": minio_config["access_key"],
            "secret_key": minio_config["secret_key"],
            "bucket": minio_config["bucket_name"],
            "default_format": "parquet",
            "default_compression": "snappy",
            "secure": False
        }
        
        minio_results = []
        for table_config in run_config["tables"]:
            run_config["current_table"] = table_config
            extracted_data = extract_data_from_mysql(run_config)
            result = load_data_to_minio(extracted_data)
            minio_results.append({
                "table": table_config["source_table"],
                "bucket": result["bucket"],
                "file": result["file_name"],
                "rows": result["rows_loaded"]
            })
        
        print(f"✅ Loaded {len(minio_results)} tables to MinIO")
        return minio_results

    @task
    def load_to_snowflake_stage(minio_results: list):
        """Load data from MinIO to Snowflake internal stage."""
        from airflow.hooks.base import BaseHook
        
        project_root = Path("/opt/airflow")
        sys.path.insert(0, str(project_root))
        
        from elt_pipeline.batch.ops.load_data_to_snowflake import load_minio_to_snowflake_via_stage_direct
        from elt_pipeline.batch.ops.extract_data_from_mysql import load_run_config, extract_data_from_mysql
        from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
        
        # Get connections and configs
        mysql_conn = BaseHook.get_connection("mysql_conn")
        minio_conn = BaseHook.get_connection("minio_conn")
        snow_conn = BaseHook.get_connection("snowflake_conn")
        
        mysql_config = get_mysql_config(mysql_conn)
        minio_config = get_minio_config(minio_conn)
        snowflake_config = get_snowflake_config(snow_conn)
        
        config_path = project_root / "elt_pipeline" / "batch" / "pipelines" / "metadata" / "table_metadata.json"
        run_config = load_run_config(str(config_path))
        
        # Override connection details from Airflow connections
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
        
        stage_results = []
        for table_config in run_config["tables"]:
            run_config["current_table"] = table_config
            
            # Extract and load to MinIO first
            extracted_data = extract_data_from_mysql(run_config)
            minio_result = load_data_to_minio(extracted_data)
            
            # Add MinIO file info to extracted data
            extracted_data["minio_file_info"] = minio_result
            
            # Load to Snowflake via stage
            result = load_minio_to_snowflake_via_stage_direct(extracted_data)
            stage_results.append({
                "table": table_config["source_table"],
                "rows": result["total_rows_loaded"],
                "stage": result["stage_name"],
                "method": result["method"]
            })
        
        print(f"✅ Loaded {len(stage_results)} tables to Snowflake")
        return stage_results

    @task
    def dbt_build(stage_results: list):
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
    extraction = extract_from_mysql(validation)
    minio_load = load_to_minio(extraction)
    snowflake_load = load_to_snowflake_stage(minio_load)
    transformation = dbt_build(snowflake_load)
    
    # Set up the pipeline flow
    validation >> extraction >> minio_load >> snowflake_load >> transformation


# Create the DAG instance
e2e_batch_elt_pipeline()