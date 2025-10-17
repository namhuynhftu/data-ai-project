"""
Unit tests for load_data_to_minio.py operations.
Tests MinIO data loading functionality with mocked dependencies.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Import the module to be tested
from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio


class TestLoadDataToMinio:
    """Test the load_data_to_minio function."""
    
    @pytest.fixture
    def sample_extracted_data(self):
        """Fixture providing sample extracted data."""
        return {
            "data": pd.DataFrame({
                'id': [1, 2, 3],
                'name': ['Alice', 'Bob', 'Charlie'],
                'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com']
            }),
            "table_config": {
                "targets": {
                    "minio": {
                        "target_table": "test_customers"
                    }
                }
            },
            "minio_target_storage": {
                "bucket": "test-bucket",
                "endpoint": "localhost:9000",
                "access_key": "testaccess",
                "secret_key": "testsecret",
                "default_format": "parquet",
                "default_compression": "snappy",
                "secure": False
            }
        }
    
    @patch('elt_pipeline.batch.ops.load_data_to_minio.pd.Timestamp')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.MinIOLoader')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.get_minio_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.BatchOperation')
    def test_load_data_success(self, mock_batch_op, mock_logger, mock_minio_loader, 
                              mock_timestamp, sample_extracted_data):
        """Test successful data loading to MinIO."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_minio_loader.return_value = mock_loader_instance
        mock_client_instance = Mock()
        mock_loader_instance.get_db_connection.return_value = mock_client_instance
        mock_loader_instance.load_data.return_value = 3  # 3 rows loaded
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Mock timestamp
        mock_timestamp_instance = Mock()
        mock_timestamp_instance.strftime.return_value = "20231001120000"
        mock_timestamp.now.return_value = mock_timestamp_instance
        
        # Execute the function
        result = load_data_to_minio(sample_extracted_data)
        
        # Assertions
        assert 'bucket' in result
        assert 'file_name' in result
        assert 'rows_loaded' in result
        
        assert result['bucket'] == 'test-bucket'
        assert result['rows_loaded'] == 3
        assert 'test_customers/test_customers_20231001120000.parquet' == result['file_name']
        
        # Verify MinIO loader initialization and method calls
        mock_minio_loader.assert_called_once_with(sample_extracted_data['minio_target_storage'])
        mock_loader_instance.get_db_connection.assert_called_once()
        mock_loader_instance.create_bucket.assert_called_once_with('test-bucket')
        
        # Verify load_data call
        mock_loader_instance.load_data.assert_called_once()
        load_call_args = mock_loader_instance.load_data.call_args
        
        # Check the data passed to load_data
        pd.testing.assert_frame_equal(load_call_args[0][0], sample_extracted_data['data'])
        
        # Check the configuration passed to load_data
        config_arg = load_call_args[0][1]
        assert config_arg['bucket'] == 'test-bucket'
        assert config_arg['file_format'] == 'parquet'
        assert config_arg['compression'] == 'snappy'
        assert 'test_customers_20231001120000.parquet' in config_arg['file_name']
        
        # Verify operation metrics were set
        assert mock_op_instance.records_processed == 3
        
        # Verify logging
        mock_logger_instance.info.assert_called_with(
            "Data successfully loaded to MinIO",
            bucket='test-bucket',
            file_name='test_customers/test_customers_20231001120000.parquet',
            rows_loaded=3,
            file_format='parquet'
        )
    
    @patch('elt_pipeline.batch.ops.load_data_to_minio.pd.Timestamp')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.MinIOLoader')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.get_minio_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.BatchOperation')
    def test_load_data_no_rows(self, mock_batch_op, mock_logger, mock_minio_loader,
                              mock_timestamp, sample_extracted_data):
        """Test loading when no rows are loaded."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_minio_loader.return_value = mock_loader_instance
        mock_client_instance = Mock()
        mock_loader_instance.get_db_connection.return_value = mock_client_instance
        mock_loader_instance.load_data.return_value = 0  # 0 rows loaded
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Mock timestamp
        mock_timestamp_instance = Mock()
        mock_timestamp_instance.strftime.return_value = "20231001120000"
        mock_timestamp.now.return_value = mock_timestamp_instance
        
        # Execute the function
        result = load_data_to_minio(sample_extracted_data)
        
        # Assertions
        assert result['rows_loaded'] == 0
        assert mock_op_instance.records_processed == 0
        
        # Verify warning is logged
        mock_logger_instance.warning.assert_called_with(
            "No data loaded to MinIO",
            bucket='test-bucket',
            file_name='test_customers/test_customers_20231001120000.parquet'
        )
    
    @patch('elt_pipeline.batch.ops.load_data_to_minio.pd.Timestamp')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.MinIOLoader')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.get_minio_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.BatchOperation')
    def test_load_data_custom_format(self, mock_batch_op, mock_logger, mock_minio_loader,
                                   mock_timestamp, sample_extracted_data):
        """Test loading with custom file format and compression."""
        # Modify config for custom format
        sample_extracted_data['minio_target_storage']['default_format'] = 'csv'
        sample_extracted_data['minio_target_storage']['default_compression'] = 'gzip'
        
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_minio_loader.return_value = mock_loader_instance
        mock_client_instance = Mock()
        mock_loader_instance.get_db_connection.return_value = mock_client_instance
        mock_loader_instance.load_data.return_value = 3
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Mock timestamp
        mock_timestamp_instance = Mock()
        mock_timestamp_instance.strftime.return_value = "20231001120000"
        mock_timestamp.now.return_value = mock_timestamp_instance
        
        # Execute the function
        result = load_data_to_minio(sample_extracted_data)
        
        # Assertions
        assert 'test_customers_20231001120000.csv' in result['file_name']
        
        # Check the configuration passed to load_data
        load_call_args = mock_loader_instance.load_data.call_args
        config_arg = load_call_args[0][1]
        assert config_arg['file_format'] == 'csv'
        assert config_arg['compression'] == 'gzip'
    
    @patch('elt_pipeline.batch.ops.load_data_to_minio.MinIOLoader')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.get_minio_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.BatchOperation')
    def test_load_data_minio_error(self, mock_batch_op, mock_logger, mock_minio_loader,
                                  sample_extracted_data):
        """Test error handling when MinIO operation fails."""
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_minio_loader.return_value = mock_loader_instance
        mock_loader_instance.get_db_connection.side_effect = Exception("MinIO connection failed")
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Execute and verify exception is raised
        with pytest.raises(Exception, match="MinIO connection failed"):
            load_data_to_minio(sample_extracted_data)
    
    @patch('elt_pipeline.batch.ops.load_data_to_minio.pd.Timestamp')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.MinIOLoader')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.get_minio_logger')
    @patch('elt_pipeline.batch.ops.load_data_to_minio.BatchOperation')
    def test_load_data_missing_config(self, mock_batch_op, mock_logger, mock_minio_loader,
                                     mock_timestamp, sample_extracted_data):
        """Test with missing configuration values (uses defaults)."""
        # Remove optional config values
        del sample_extracted_data['minio_target_storage']['default_format']
        del sample_extracted_data['minio_target_storage']['default_compression']
        
        # Setup mocks
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        mock_loader_instance = Mock()
        mock_minio_loader.return_value = mock_loader_instance
        mock_client_instance = Mock()
        mock_loader_instance.get_db_connection.return_value = mock_client_instance
        mock_loader_instance.load_data.return_value = 3
        
        mock_op_instance = Mock()
        mock_batch_op.return_value.__enter__ = Mock(return_value=mock_op_instance)
        mock_batch_op.return_value.__exit__ = Mock(return_value=None)
        
        # Mock timestamp
        mock_timestamp_instance = Mock()
        mock_timestamp_instance.strftime.return_value = "20231001120000"
        mock_timestamp.now.return_value = mock_timestamp_instance
        
        # Execute the function
        result = load_data_to_minio(sample_extracted_data)
        
        # Verify defaults are used
        load_call_args = mock_loader_instance.load_data.call_args
        config_arg = load_call_args[0][1]
        assert config_arg['file_format'] == 'parquet'  # default
        assert config_arg['compression'] == 'snappy'   # default
        
        # Verify file name uses default format
        assert 'test_customers_20231001120000.parquet' in result['file_name']


if __name__ == "__main__":
    pytest.main([__file__])