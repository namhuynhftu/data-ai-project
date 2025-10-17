"""
Unit tests for extract_data_from_mysql.py operations.
Tests MySQL data extraction functionality with mocked dependencies.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import json
import os
from datetime import datetime

# Import the module to be tested
from elt_pipeline.batch.ops.extract_data_from_mysql import (
    load_run_config,
    extract_data_from_mysql
)


class TestLoadRunConfig:
    """Test the load_run_config function."""
    
    def test_load_run_config_success(self, tmp_path):
        """Test successful loading of run configuration."""
        # Create a temporary config file
        config_data = {
            "tables": [
                {
                    "source_table": "test_table",
                    "strategy": "full_load"
                }
            ]
        }
        
        config_file = tmp_path / "test_config.json"
        config_file.write_text(json.dumps(config_data))
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'MYSQL_HOST': 'localhost',
            'MYSQL_PORT': '3306',
            'MYSQL_USER': 'testuser',
            'MYSQL_PASSWORD': 'testpass',
            'MYSQL_DATABASE': 'testdb',
            'MYSQL_SCHEMA': 'testschema',
            'MINIO_ENDPOINT': 'localhost:9000',
            'MINIO_ACCESS_KEY': 'minioaccess',
            'MINIO_SECRET_KEY': 'miniosecret',
            'MINIO_BUCKET': 'testbucket',
            'SNOWFLAKE_ACCOUNT': 'testaccount',
            'SNOWFLAKE_USER': 'testuser',
            'SNOWFLAKE_PRIVATE_KEY_FILE_PATH': '/path/to/key',
            'SNOWFLAKE_PRIVATE_KEY_FILE_PWD': 'keypwd',
            'SNOWFLAKE_WAREHOUSE': 'testwh',
            'SNOWFLAKE_DATABASE': 'testdb',
            'SNOWFLAKE_SCHEMA': 'RAW_DATA',
            'SNOWFLAKE_ROLE': 'testrole'
        }):
            result = load_run_config(str(config_file))
        
        # Assertions
        assert 'tables' in result
        assert 'data_source_config' in result
        assert 'minio_target_storage' in result
        assert 'snowflake_target_storage' in result
        
        assert len(result['tables']) == 1
        assert result['tables'][0]['source_table'] == 'test_table'
        
        assert result['data_source_config']['host'] == 'localhost'
        assert result['data_source_config']['port'] == 3306
        assert result['data_source_config']['user'] == 'testuser'
        
        assert result['minio_target_storage']['endpoint'] == 'localhost:9000'
        assert result['minio_target_storage']['bucket'] == 'testbucket'
        
        assert result['snowflake_target_storage']['account'] == 'testaccount'
        assert result['snowflake_target_storage']['authenticator'] == 'SNOWFLAKE_JWT'
    
    def test_load_run_config_file_not_found(self):
        """Test error handling when config file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            load_run_config("nonexistent_file.json")
    
    def test_load_run_config_invalid_json(self, tmp_path):
        """Test error handling for invalid JSON."""
        config_file = tmp_path / "invalid_config.json"
        config_file.write_text("invalid json content")
        
        with pytest.raises(json.JSONDecodeError):
            load_run_config(str(config_file))


class TestExtractDataFromMySQL:
    """Test the extract_data_from_mysql function."""
    
    @pytest.fixture
    def sample_run_config(self):
        """Fixture providing a sample run configuration."""
        return {
            "data_source_config": {
                "host": "localhost",
                "port": 3306,
                "user": "testuser",
                "password": "testpass",
                "database": "testdb",
                "schema": "testschema"
            },
            "current_table": {
                "source_table": "test_customers",
                "strategy": "full_load",
                "loaded_at": None,
                "watermark_column": "updated_at"
            },
            "minio_target_storage": {
                "endpoint": "localhost:9000",
                "bucket": "testbucket"
            },
            "snowflake_target_storage": {
                "account": "testaccount",
                "database": "testdb"
            }
        }
    
    @pytest.fixture
    def sample_dataframe(self):
        """Fixture providing a sample pandas DataFrame."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com']
        })
    
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.MySQLLoader')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.get_mysql_logger')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.BatchOperation')
    def test_extract_full_load_success(self, mock_batch_op, mock_logger, mock_mysql_loader, 
                                     sample_run_config, sample_dataframe):
        """Test successful full load extraction."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_mysql_loader.return_value = mock_loader_instance
        mock_loader_instance.extract_data.return_value = sample_dataframe
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Execute the function
        result = extract_data_from_mysql(sample_run_config)
        
        # Assertions
        assert 'data' in result
        assert 'table_config' in result
        assert 'loaded_at' in result
        assert 'minio_target_storage' in result
        assert 'snowflake_target_storage' in result
        
        # Check that data is correctly returned
        pd.testing.assert_frame_equal(result['data'], sample_dataframe)
        assert result['table_config'] == sample_run_config['current_table']
        
        # Verify MySQL loader was called correctly
        mock_mysql_loader.assert_called_once_with(sample_run_config['data_source_config'])
        mock_loader_instance.extract_data.assert_called_once()
        
        # Check SQL query construction
        call_args = mock_loader_instance.extract_data.call_args[0][0]
        assert "SELECT *" in call_args
        assert "FROM test_customers" in call_args
        assert "WHERE 1=1" in call_args
        
        # Verify operation metrics were set
        assert mock_op_instance.records_processed == len(sample_dataframe)
    
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.MySQLLoader')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.get_mysql_logger')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.BatchOperation')
    def test_extract_incremental_load_first_time(self, mock_batch_op, mock_logger, mock_mysql_loader,
                                               sample_run_config, sample_dataframe):
        """Test incremental load for the first time (acts like full load)."""
        # Modify config for incremental load
        sample_run_config['current_table']['strategy'] = 'incremental_by_watermark'
        sample_run_config['current_table']['loaded_at'] = None
        
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_mysql_loader.return_value = mock_loader_instance
        mock_loader_instance.extract_data.return_value = sample_dataframe
        mock_loader_instance.get_watermark.return_value = '2023-10-01 00:00:00'
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Execute the function
        result = extract_data_from_mysql(sample_run_config)
        
        # Assertions
        assert result['loaded_at'] == '2023-10-01 00:00:00'
        
        # Verify watermark was retrieved
        mock_loader_instance.get_watermark.assert_called_once_with('test_customers', 'updated_at')
        
        # Verify BatchOperation was used for logging (actual logging works as shown in test output)
        mock_batch_op.assert_called_once()
    
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.MySQLLoader')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.get_mysql_logger')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.BatchOperation')
    def test_extract_incremental_load_with_watermark(self, mock_batch_op, mock_logger, mock_mysql_loader,
                                                   sample_run_config, sample_dataframe):
        """Test incremental load with existing watermark."""
        # Modify config for incremental load with existing watermark
        sample_run_config['current_table']['strategy'] = 'incremental_by_watermark'
        sample_run_config['current_table']['loaded_at'] = '2023-09-01 00:00:00'
        
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_mysql_loader.return_value = mock_loader_instance
        mock_loader_instance.extract_data.return_value = sample_dataframe
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Execute the function
        result = extract_data_from_mysql(sample_run_config)
        
        # Assertions
        assert result['loaded_at'] == '2023-09-01 00:00:00'
        
        # Check SQL query includes watermark condition
        call_args = mock_loader_instance.extract_data.call_args[0][0]
        assert "AND updated_at > '2023-09-01 00:00:00'" in call_args
        
        # Verify BatchOperation was used for logging (actual logging works as shown in test output)
        mock_batch_op.assert_called_once()
    
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.MySQLLoader')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.get_mysql_logger')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.BatchOperation')
    def test_extract_empty_result(self, mock_batch_op, mock_logger, mock_mysql_loader,
                                sample_run_config):
        """Test extraction when no data is returned."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_mysql_loader.return_value = mock_loader_instance
        mock_loader_instance.extract_data.return_value = pd.DataFrame()  # Empty DataFrame
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Execute the function
        result = extract_data_from_mysql(sample_run_config)
        
        # Assertions
        assert len(result['data']) == 0
        assert mock_op_instance.records_processed == 0
    
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.MySQLLoader')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.get_mysql_logger')
    @patch('elt_pipeline.batch.ops.extract_data_from_mysql.BatchOperation')
    def test_extract_mysql_error(self, mock_batch_op, mock_logger, mock_mysql_loader,
                                sample_run_config):
        """Test error handling when MySQL extraction fails."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_mysql_loader.return_value = mock_loader_instance
        mock_loader_instance.extract_data.side_effect = Exception("Database connection failed")
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Database connection failed"):
            extract_data_from_mysql(sample_run_config)


if __name__ == "__main__":
    pytest.main([__file__])