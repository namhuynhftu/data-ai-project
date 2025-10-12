#!/usr/bin/env python3
"""
Integration tests for batch pipeline operations.

Tests the extract, load operations with mock data to ensure
the pipeline components work together correctly.
"""

import pytest
import pandas as pd
import json
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from elt_pipeline.batch.ops.extract_data_from_mysql import extract_data_from_mysql, load_run_config
from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
from elt_pipeline.batch.ops.load_data_to_snowflake import load_data_to_snowflake


class TestBatchOperationsIntegration:
    """Integration tests for batch pipeline operations."""
    
    @pytest.fixture
    def sample_metadata(self):
        """Sample metadata configuration for testing."""
        return {
            "tables": [
                {
                    "source_table": "customers",
                    "strategy": "full_load",
                    "loaded_at": None,
                    "watermark_column": "created_at",
                    "targets": {
                        "minio": {
                            "target_table": "customers"
                        },
                        "snowflake": {
                            "target_table": "customers"
                        }
                    }
                }
            ]
        }
    
    @pytest.fixture
    def sample_run_config(self):
        """Sample run configuration for testing."""
        return {
            "tables": [
                {
                    "source_table": "customers",
                    "strategy": "full_load",
                    "loaded_at": None,
                    "watermark_column": "created_at",
                    "targets": {
                        "minio": {"target_table": "customers"},
                        "snowflake": {"target_table": "customers"}
                    }
                }
            ],
            "data_source_config": {
                "host": "localhost",
                "port": 3306,
                "user": "test_user",
                "password": "test_password",
                "database": "test_db",
                "schema": "test_schema"
            },
            "minio_target_storage": {
                "endpoint": "localhost:9000",
                "access_key": "test_access",
                "secret_key": "test_secret",
                "bucket": "test-bucket",
                "default_format": "parquet",
                "default_compression": "snappy",
                "secure": False
            },
            "snowflake_target_storage": {
                "account": "test_account",
                "user": "test_user",
                "warehouse": "test_warehouse",
                "database": "test_database",
                "schema": "RAW_DATA",
                "role": "test_role"
            }
        }
    
    @pytest.fixture
    def sample_customer_data(self):
        """Sample customer data for testing."""
        return pd.DataFrame({
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'city': ['New York', 'Los Angeles', 'Chicago'],
            'state': ['NY', 'CA', 'IL'],
            'created_at': ['2023-01-01', '2023-01-02', '2023-01-03']
        })
    
    @patch('builtins.open')
    @patch('json.load')
    @patch('os.getenv')
    def test_load_run_config(self, mock_getenv, mock_json_load, mock_open, sample_metadata):
        """Test loading run configuration from metadata file."""
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            "MYSQL_HOST": "localhost",
            "MYSQL_PORT": "3306",
            "MYSQL_USER": "test_user",
            "MYSQL_PASSWORD": "test_password",
            "MYSQL_DATABASE": "test_db",
            "MYSQL_SCHEMA": "test_schema",
            "MINIO_ENDPOINT": "localhost:9000",
            "MINIO_ROOT_USER": "test_access",
            "MINIO_ROOT_PASSWORD": "test_secret",
            "MINIO_BUCKET": "test-bucket",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_WAREHOUSE": "test_warehouse",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_SCHEMA": "RAW_DATA",
            "SNOWFLAKE_ROLE": "test_role"
        }.get(key, default)
        
        # Mock file operations
        mock_json_load.return_value = sample_metadata
        
        # Test loading configuration
        config_path = "test_metadata.json"
        result = load_run_config(config_path)
        
        # Assertions
        assert "tables" in result
        assert "data_source_config" in result
        assert "minio_target_storage" in result
        assert "snowflake_target_storage" in result
        
        # Verify data source config
        data_source = result["data_source_config"]
        assert data_source["host"] == "localhost"
        assert data_source["port"] == 3306
        assert data_source["database"] == "test_db"
        
        # Verify MinIO config
        minio_config = result["minio_target_storage"]
        assert minio_config["endpoint"] == "localhost:9000"
        assert minio_config["bucket"] == "test-bucket"
        
        # Verify Snowflake config
        snowflake_config = result["snowflake_target_storage"]
        assert snowflake_config["account"] == "test_account"
        assert snowflake_config["schema"] == "RAW_DATA"
    
    @patch('elt_pipeline.batch.utils.mysql_loader.MySQLLoader')
    def test_extract_data_from_mysql_full_load(self, mock_mysql_loader_class, 
                                              sample_run_config, sample_customer_data):
        """Test MySQL data extraction with full load strategy."""
        # Mock MySQL loader
        mock_loader = Mock()
        mock_loader.extract_data.return_value = sample_customer_data
        mock_mysql_loader_class.return_value = mock_loader
        
        # Prepare run config with current table
        table_config = sample_run_config["tables"][0]
        run_config_with_table = {
            **sample_run_config,
            "current_table": table_config
        }
        
        # Test extraction
        result = extract_data_from_mysql(run_config_with_table)
        
        # Assertions
        assert "data" in result
        assert "table_config" in result
        assert "loaded_at" in result
        assert "minio_target_storage" in result
        assert "snowflake_target_storage" in result
        
        pd.testing.assert_frame_equal(result["data"], sample_customer_data)
        assert result["table_config"] == table_config
        
        # Verify SQL query for full load
        extract_call = mock_loader.extract_data.call_args[0][0]
        assert "SELECT *" in extract_call
        assert "FROM customers" in extract_call
        assert "WHERE 1=1" in extract_call
    
    @patch('elt_pipeline.batch.utils.mysql_loader.MySQLLoader')
    def test_extract_data_from_mysql_incremental_load(self, mock_mysql_loader_class, 
                                                     sample_run_config, sample_customer_data):
        """Test MySQL data extraction with incremental load strategy."""
        # Mock MySQL loader
        mock_loader = Mock()
        mock_loader.extract_data.return_value = sample_customer_data
        mock_mysql_loader_class.return_value = mock_loader
        
        # Modify config for incremental load
        table_config = sample_run_config["tables"][0].copy()
        table_config["strategy"] = "incremental_by_watermark"
        table_config["loaded_at"] = "2023-01-01 00:00:00"
        
        run_config_with_table = {
            **sample_run_config,
            "current_table": table_config
        }
        
        # Test extraction
        result = extract_data_from_mysql(run_config_with_table)
        
        # Assertions
        pd.testing.assert_frame_equal(result["data"], sample_customer_data)
        
        # Verify SQL query includes watermark condition
        extract_call = mock_loader.extract_data.call_args[0][0]
        assert "created_at > '2023-01-01 00:00:00'" in extract_call
    
    @patch('elt_pipeline.batch.utils.minio_loader.MinIOLoader')
    @patch('pandas.Timestamp')
    def test_load_data_to_minio(self, mock_timestamp, mock_minio_loader_class, 
                               sample_customer_data):
        """Test data loading to MinIO."""
        # Mock timestamp
        mock_timestamp.now.return_value.strftime.return_value = "20231201120000"
        
        # Mock MinIO loader
        mock_loader = Mock()
        mock_loader.load_data.return_value = len(sample_customer_data)
        mock_minio_loader_class.return_value = mock_loader
        
        # Prepare extracted data
        extracted_data = {
            "data": sample_customer_data,
            "table_config": {
                "targets": {
                    "minio": {"target_table": "customers"}
                }
            },
            "minio_target_storage": {
                "bucket": "test-bucket",
                "default_format": "parquet",
                "default_compression": "snappy"
            }
        }
        
        # Test loading to MinIO
        result = load_data_to_minio(extracted_data)
        
        # Assertions
        assert "bucket" in result
        assert "file_name" in result
        assert "rows_loaded" in result
        
        assert result["bucket"] == "test-bucket"
        assert result["rows_loaded"] == len(sample_customer_data)
        assert "customers/customers_20231201120000.parquet" == result["file_name"]
        
        # Verify loader methods were called
        mock_loader.create_bucket.assert_called_once_with("test-bucket")
        mock_loader.load_data.assert_called_once()
    
    @patch('snowflake.connector.connect')
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('elt_pipeline.batch.utils.minio_loader.MinIOLoader')
    def test_load_data_to_snowflake(self, mock_minio_loader_class, mock_write_pandas, 
                                   mock_snowflake_connect, sample_customer_data):
        """Test data loading to Snowflake."""
        # Mock MinIO loader for reading data
        mock_minio_loader = Mock()
        mock_minio_client = Mock()
        mock_response = Mock()
        
        # Create mock parquet data
        import io
        buffer = io.BytesIO()
        sample_customer_data.to_parquet(buffer)
        buffer.seek(0)
        mock_response.read.return_value = buffer.getvalue()
        
        mock_minio_client.get_object.return_value = mock_response
        mock_minio_loader.get_db_connection.return_value = mock_minio_client
        mock_minio_loader_class.return_value = mock_minio_loader
        
        # Mock Snowflake connection and write_pandas
        mock_conn = Mock()
        mock_snowflake_connect.return_value = mock_conn
        mock_write_pandas.return_value = (True, 1, len(sample_customer_data), None)
        
        # Prepare data with MinIO info
        data_with_minio_info = {
            "table_config": {
                "targets": {
                    "snowflake": {"target_table": "customers"}
                }
            },
            "minio_target_storage": {
                "endpoint": "localhost:9000",
                "access_key": "test_access",
                "secret_key": "test_secret"
            },
            "minio_file_info": {
                "bucket": "test-bucket",
                "file_name": "customers/customers_20231201120000.parquet"
            }
        }
        
        # Test loading to Snowflake
        load_data_to_snowflake(data_with_minio_info)
        
        # Assertions
        mock_minio_client.get_object.assert_called_once_with(
            "test-bucket", 
            "customers/customers_20231201120000.parquet"
        )
        mock_snowflake_connect.assert_called_once()
        mock_write_pandas.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.extract_data_from_mysql')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.load_data_to_minio')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.load_data_to_snowflake')
    def test_end_to_end_pipeline_flow(self, mock_snowflake_load, mock_minio_load, 
                                     mock_mysql_extract, sample_run_config, sample_customer_data):
        """Test end-to-end pipeline flow with mocked operations."""
        # Mock extraction result
        extract_result = {
            "data": sample_customer_data,
            "table_config": sample_run_config["tables"][0],
            "loaded_at": None,
            "minio_target_storage": sample_run_config["minio_target_storage"],
            "snowflake_target_storage": sample_run_config["snowflake_target_storage"]
        }
        mock_mysql_extract.return_value = extract_result
        
        # Mock MinIO result
        minio_result = {
            "bucket": "test-bucket",
            "file_name": "customers/customers_20231201120000.parquet",
            "rows_loaded": len(sample_customer_data)
        }
        mock_minio_load.return_value = minio_result
        
        # Simulate pipeline execution
        table_config = sample_run_config["tables"][0]
        table_run_config = {
            **sample_run_config,
            "current_table": table_config
        }
        
        # Step 1: Extract
        data = mock_mysql_extract(table_run_config)
        
        # Step 2: Load to MinIO
        minio_result = mock_minio_load(data)
        
        # Step 3: Load to Snowflake
        data_with_minio_info = {
            **data,
            "minio_file_info": minio_result
        }
        mock_snowflake_load(data_with_minio_info)
        
        # Assertions
        mock_mysql_extract.assert_called_once_with(table_run_config)
        mock_minio_load.assert_called_once_with(extract_result)
        mock_snowflake_load.assert_called_once_with(data_with_minio_info)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])