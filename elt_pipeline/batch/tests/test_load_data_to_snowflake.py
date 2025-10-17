"""
Unit tests for load_data_to_snowflake.py operations.
Tests Snowflake data loading functionality with mocked dependencies.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
import os

# Import the module to be tested
from elt_pipeline.batch.ops.load_data_to_snowflake import (
    load_minio_to_snowflake_via_stage_direct,
    load_data_to_snowflake_stage_direct,
    load_data_from_snowflake_stage_to_table,
    _load_direct_from_minio,
    _load_via_temp_file,
    _download_from_minio_to_temp,
    _load_temp_file_to_snowflake_stage
)


class TestLoadMinioToSnowflakeViaStageMain:
    """Test the main orchestration function."""
    
    @pytest.fixture
    def sample_extracted_data(self):
        """Fixture providing sample extracted data."""
        return {
            "table_config": {
                "targets": {
                    "snowflake": {
                        "target_table": "test_customers"
                    }
                }
            },
            "data": pd.DataFrame({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie']
            }),
            "minio_target_storage": {
                "endpoint": "localhost:9000",
                "bucket": "test-bucket"
            }
        }
    
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.get_snowflake_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.load_data_to_snowflake_stage_direct')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.load_data_from_snowflake_stage_to_table')
    def test_successful_load_workflow(self, mock_stage_to_table, mock_load_stage, 
                                     mock_logger, sample_extracted_data):
        """Test successful complete workflow."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_load_stage.return_value = {
            "success": True,
            "stage_name": "MINIO_STAGE_SHARED",
            "method": "temp_file_upload",
            "already_loaded_to_table": False
        }
        
        mock_stage_to_table.return_value = {
            "success": True,
            "rows_loaded": 100,
            "files_processed": 1
        }
        
        # Execute the function
        result = load_minio_to_snowflake_via_stage_direct(sample_extracted_data)
        
        # Assertions
        assert result["success"] is True
        assert result["total_rows_loaded"] == 100
        assert result["stage_name"] == "MINIO_STAGE_SHARED"
        assert result["method"] == "temp_file_upload"
        
        # Verify function calls
        mock_load_stage.assert_called_once_with(sample_extracted_data)
        mock_stage_to_table.assert_called_once_with(sample_extracted_data, mock_load_stage.return_value)
    
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.get_snowflake_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.load_data_to_snowflake_stage_direct')
    def test_stage_loading_failure(self, mock_load_stage, mock_logger, sample_extracted_data):
        """Test failure in stage loading."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_load_stage.return_value = {"success": False}
        
        # Execute and verify exception
        with pytest.raises(Exception, match="Failed to load data to Snowflake stage"):
            load_minio_to_snowflake_via_stage_direct(sample_extracted_data)
    
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.get_snowflake_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.load_data_to_snowflake_stage_direct')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.load_data_from_snowflake_stage_to_table')
    def test_already_loaded_to_table(self, mock_stage_to_table, mock_load_stage, 
                                    mock_logger, sample_extracted_data):
        """Test when data is already loaded to table during stage loading."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_load_stage.return_value = {
            "success": True,
            "stage_name": "MINIO_STAGE_SHARED",
            "already_loaded_to_table": True,
            "rows_staged": 50,
            "files_copied": 1
        }
        
        # Execute the function
        result = load_minio_to_snowflake_via_stage_direct(sample_extracted_data)
        
        # Assertions
        assert result["success"] is True
        assert result["total_rows_loaded"] == 50
        
        # Verify stage_to_table was NOT called
        mock_stage_to_table.assert_not_called()


class TestLoadDataToSnowflakeStage:
    """Test the stage loading function."""
    
    @pytest.fixture
    def sample_extracted_data(self):
        """Fixture providing sample extracted data."""
        return {
            "table_config": {
                "targets": {
                    "snowflake": {
                        "target_table": "test_customers"
                    }
                }
            },
            "data": pd.DataFrame({'id': [1, 2, 3]})
        }
    
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.get_snowflake_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake._load_direct_from_minio')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake._load_via_temp_file')
    def test_fallback_to_temp_file(self, mock_temp_file, mock_direct_minio, 
                                  mock_logger, sample_extracted_data):
        """Test fallback to temp file when direct MinIO fails."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Direct MinIO should fail
        mock_direct_minio.side_effect = Exception("Direct MinIO not supported")
        
        # Temp file should succeed
        mock_temp_file.return_value = {
            "success": True,
            "stage_name": "MINIO_STAGE_SHARED"
        }
        
        # Execute the function
        result = load_data_to_snowflake_stage_direct(sample_extracted_data)
        
        # Assertions
        assert result["success"] is True
        assert result["stage_name"] == "MINIO_STAGE_SHARED"
        
        # Verify fallback was used
        mock_direct_minio.assert_called_once()
        mock_temp_file.assert_called_once()
        
        # Verify fallback logging
        mock_logger_instance.info.assert_called_with(
            "Using temp file fallback",
            table="test_customers"
        )


class TestLoadDirectFromMinio:
    """Test the _load_direct_from_minio function."""
    
    def test_direct_minio_not_supported(self):
        """Test that direct MinIO loading raises exception."""
        with pytest.raises(Exception, match="Direct MinIO loading not supported for localhost"):
            _load_direct_from_minio({}, Mock())


class TestLoadViaTempFile:
    """Test the temp file loading workflow."""
    
    @pytest.fixture
    def sample_extracted_data(self):
        """Fixture for temp file tests."""
        return {
            "table_config": {
                "targets": {
                    "snowflake": {
                        "target_table": "test_customers"
                    }
                }
            },
            "minio_target_storage": {
                "endpoint": "localhost:9000",
                "access_key": "testkey",
                "secret_key": "testsecret",
                "secure": False
            },
            "minio_file_info": {
                "bucket": "test-bucket",
                "file_name": "customers/customers_123.parquet",
                "rows_loaded": 100
            }
        }
    
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.Path')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.datetime')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake._download_from_minio_to_temp')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake._load_temp_file_to_snowflake_stage')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.BatchOperation')
    def test_temp_file_workflow_success(self, mock_batch_op, mock_load_temp, mock_download,
                                       mock_datetime, mock_path, sample_extracted_data):
        """Test successful temp file workflow."""
        # Setup mocks
        mock_logger = Mock()
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        mock_datetime.now.return_value.strftime.return_value = "20231001_120000"
        
        mock_temp_dir = Mock()
        mock_temp_file = Mock()
        mock_temp_file.exists.return_value = True
        mock_temp_dir.__truediv__ = Mock(return_value=mock_temp_file)
        mock_path.return_value = mock_temp_dir
        mock_temp_dir.mkdir = Mock()
        
        mock_load_temp.return_value = {
            "success": True,
            "stage_name": "MINIO_STAGE_SHARED"
        }
        
        # Execute the function
        result = _load_via_temp_file(sample_extracted_data, mock_logger)
        
        # Assertions
        assert result["success"] is True
        assert result["stage_name"] == "MINIO_STAGE_SHARED"
        
        # Verify workflow steps
        mock_temp_dir.mkdir.assert_called_with(exist_ok=True)
        mock_download.assert_called_once()
        mock_load_temp.assert_called_once()
        mock_temp_file.unlink.assert_called_once()  # Cleanup
        
        # Verify operation metrics
        assert mock_op_instance.records_processed == 100


class TestDownloadFromMinioToTemp:
    """Test MinIO download functionality."""
    
    @patch('builtins.__import__')
    def test_download_success(self, mock_import):
        """Test successful download from MinIO to temp file."""
        # Setup mocks
        mock_client = Mock()
        mock_minio_class = Mock()
        mock_minio_class.return_value = mock_client
        
        # Mock the minio import
        def mock_import_func(name, *args, **kwargs):
            if name == 'minio':
                mock_minio_module = Mock()
                mock_minio_module.Minio = mock_minio_class
                return mock_minio_module
            else:
                # Call the original import for other modules
                return __import__(name, *args, **kwargs)
        
        mock_import.side_effect = mock_import_func
        
        minio_config = {
            "endpoint": "localhost:9000",
            "access_key": "testkey",
            "secret_key": "testsecret",
            "secure": False
        }
        
        minio_file_info = {
            "bucket": "test-bucket",
            "file_name": "customers/data.parquet"
        }
        
        temp_file_path = Path("/tmp/test_file.parquet")
        mock_logger = Mock()
        
        # Execute the function
        _download_from_minio_to_temp(minio_config, minio_file_info, temp_file_path, mock_logger)
        
        # Verify MinIO client creation
        mock_minio_class.assert_called_once_with(
            "localhost:9000",
            access_key="testkey",
            secret_key="testsecret",
            secure=False
        )
        
        # Verify file download
        mock_client.fget_object.assert_called_once_with(
            "test-bucket",
            "customers/data.parquet",
            str(temp_file_path)
        )
    
    @patch('builtins.__import__')
    def test_download_with_port_parsing(self, mock_import):
        """Test endpoint parsing when port is included."""
        # Setup mocks
        mock_client = Mock()
        mock_minio_class = Mock()
        mock_minio_class.return_value = mock_client
        
        # Mock the minio import
        def mock_import_func(name, *args, **kwargs):
            if name == 'minio':
                mock_minio_module = Mock()
                mock_minio_module.Minio = mock_minio_class
                return mock_minio_module
            else:
                return __import__(name, *args, **kwargs)
        
        mock_import.side_effect = mock_import_func
        
        minio_config = {
            "endpoint": "minio.example.com:9000",
            "access_key": "testkey",
            "secret_key": "testsecret"
        }
        
        minio_file_info = {
            "bucket": "test-bucket",
            "file_name": "data.parquet"
        }
        
        temp_file_path = Path("/tmp/test_file.parquet")
        mock_logger = Mock()
        
        # Execute the function
        _download_from_minio_to_temp(minio_config, minio_file_info, temp_file_path, mock_logger)
        
        # Verify correct endpoint parsing
        mock_minio_class.assert_called_once_with(
            "minio.example.com:9000",
            access_key="testkey",
            secret_key="testsecret",
            secure=False  # Default value
        )


class TestLoadTempFileToSnowflakeStage:
    """Test loading temp file to Snowflake stage."""
    
    @patch.dict(os.environ, {
        'SNOWFLAKE_ACCOUNT': 'testaccount',
        'SNOWFLAKE_USER': 'testuser',
        'SNOWFLAKE_PRIVATE_KEY_FILE_PATH': '/path/to/key',
        'SNOWFLAKE_PRIVATE_KEY_FILE_PWD': 'password',
        'SNOWFLAKE_WAREHOUSE': 'testwh',
        'SNOWFLAKE_DATABASE': 'testdb',
        'SNOWFLAKE_SCHEMA': 'RAW_DATA',
        'SNOWFLAKE_ROLE': 'testrole'
    })
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.SnowflakeLoader')
    def test_temp_file_to_stage_success(self, mock_snowflake_loader):
        """Test successful temp file upload to Snowflake stage."""
        # Setup mocks
        mock_loader_instance = Mock()
        mock_snowflake_loader.return_value = mock_loader_instance
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_loader_instance.get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock PUT command results
        mock_cursor.fetchall.return_value = [
            ("file1.parquet", "SUCCESS", 1024, 1024, "UPLOADED", ""),
        ]
        
        temp_file_path = Path("/tmp/test_customers_123.parquet")
        table_name = "test_customers"
        mock_logger = Mock()
        
        # Execute the function
        result = _load_temp_file_to_snowflake_stage(temp_file_path, table_name, mock_logger)
        
        # Assertions
        assert result["success"] is True
        assert result["stage_name"] == "MINIO_STAGE_SHARED"
        assert result["method"] == "temp_file_upload"
        assert result["files_copied"] == 1
        assert result["database"] == "testdb"
        assert result["schema"] == "RAW_DATA"
        
        # Verify Snowflake operations
        mock_loader_instance.get_db_connection.assert_called_once()
        
        # Verify stage creation
        create_stage_calls = [call for call in mock_cursor.execute.call_args_list 
                             if 'CREATE STAGE' in str(call)]
        assert len(create_stage_calls) == 1
        
        # Verify PUT command
        put_calls = [call for call in mock_cursor.execute.call_args_list 
                    if 'PUT file://' in str(call)]
        assert len(put_calls) == 1
        
        # Verify cleanup
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestLoadDataFromSnowflakeStageToTable:
    """Test loading from Snowflake stage to final table."""
    
    @pytest.fixture
    def sample_extracted_data(self):
        """Fixture for stage to table tests."""
        return {
            "table_config": {
                "targets": {
                    "snowflake": {
                        "target_table": "test_customers"
                    }
                }
            },
            "add_ingestion_column": True,
            "ingestion_date": "2023-10-01T12:00:00"
        }
    
    @pytest.fixture
    def sample_stage_info(self):
        """Fixture for stage info."""
        return {
            "stage_name": "MINIO_STAGE_SHARED",
            "database": "testdb",
            "schema": "RAW_DATA"
        }
    
    @patch.dict(os.environ, {
        'SNOWFLAKE_ACCOUNT': 'testaccount',
        'SNOWFLAKE_USER': 'testuser',
        'SNOWFLAKE_PRIVATE_KEY_FILE_PATH': '/path/to/key',
        'SNOWFLAKE_PRIVATE_KEY_FILE_PWD': 'password',
        'SNOWFLAKE_WAREHOUSE': 'testwh',
        'SNOWFLAKE_DATABASE': 'testdb',
        'SNOWFLAKE_SCHEMA': 'RAW_DATA',
        'SNOWFLAKE_ROLE': 'testrole'
    })
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.SnowflakeLoader')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.get_snowflake_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_snowflake.BatchOperation')
    def test_stage_to_table_success(self, mock_batch_op, mock_logger, mock_snowflake_loader,
                                   sample_extracted_data, sample_stage_info):
        """Test successful stage to table loading."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        mock_loader_instance = Mock()
        mock_snowflake_loader.return_value = mock_loader_instance
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_loader_instance.get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock column check result (column doesn't exist)
        mock_cursor.fetchone.return_value = [0]  # Column doesn't exist
        
        # Mock COPY INTO results
        mock_cursor.fetchall.return_value = [
            ("file1.parquet", "SUCCESS", 100, 100, 0, 0, "LOADED", ""),
        ]
        
        # Execute the function
        result = load_data_from_snowflake_stage_to_table(sample_extracted_data, sample_stage_info)
        
        # Assertions
        assert result["success"] is True
        assert result["rows_loaded"] == 100
        assert result["files_processed"] == 1
        
        # Verify operation metrics
        assert mock_op_instance.records_processed == 100
        
        # Verify SQL operations were called
        cursor_calls = mock_cursor.execute.call_args_list
        
        # Should have: TRUNCATE, column check, ALTER TABLE, COPY INTO, UPDATE
        truncate_calls = [call for call in cursor_calls if 'TRUNCATE' in str(call)]
        assert len(truncate_calls) == 1
        
        copy_calls = [call for call in cursor_calls if 'COPY INTO' in str(call)]
        assert len(copy_calls) == 1
        
        alter_calls = [call for call in cursor_calls if 'ALTER TABLE' in str(call)]
        assert len(alter_calls) == 1  # Add ingestion_date column
        
        update_calls = [call for call in cursor_calls if 'UPDATE' in str(call)]
        assert len(update_calls) == 1  # Set ingestion_date


if __name__ == "__main__":
    pytest.main([__file__])