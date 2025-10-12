#!/usr/bin/env python3
"""
Unit tests for MinIO loader functionality.

Tests the MinIOLoader class methods including connection handling,
bucket creation, and data loading functionality.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from io import BytesIO
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from elt_pipeline.batch.utils.minio_loader import MinIOLoader


class TestMinIOLoader:
    """Test cases for MinIOLoader class."""
    
    @pytest.fixture
    def minio_params(self):
        """Sample MinIO connection parameters for testing."""
        return {
            'endpoint': 'localhost:9000',
            'access_key': 'test_access_key',
            'secret_key': 'test_secret_key',
            'secure': False
        }
    
    @pytest.fixture
    def minio_loader(self, minio_params):
        """Create MinIOLoader instance for testing."""
        return MinIOLoader(minio_params)
    
    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'product_name': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
            'price': [10.99, 25.50, 8.75, 15.00, 32.25],
            'category': ['Electronics', 'Clothing', 'Books', 'Electronics', 'Clothing']
        })
    
    def test_init(self, minio_loader, minio_params):
        """Test MinIOLoader initialization."""
        assert minio_loader.params == minio_params
        assert minio_loader.client is None
        assert hasattr(minio_loader, 'logger')
    
    @patch('elt_pipeline.batch.utils.minio_loader.Minio')
    def test_get_db_connection_success(self, mock_minio_class, minio_loader):
        """Test successful MinIO connection."""
        # Mock MinIO client
        mock_client = Mock()
        mock_client.list_buckets.return_value = []
        mock_minio_class.return_value = mock_client
        
        # Test connection
        result = minio_loader.get_db_connection()
        
        # Assertions
        assert result == mock_client
        assert minio_loader.client == mock_client
        mock_minio_class.assert_called_once_with(
            endpoint='localhost:9000',
            access_key='test_access_key',
            secret_key='test_secret_key',
            secure=False
        )
        mock_client.list_buckets.assert_called_once()
    
    @patch('elt_pipeline.batch.utils.minio_loader.Minio')
    def test_get_db_connection_failure(self, mock_minio_class, minio_loader):
        """Test MinIO connection failure."""
        from minio.error import S3Error
        mock_minio_class.side_effect = S3Error("Connection failed", None, None, None, None, None)
        
        with pytest.raises(S3Error):
            minio_loader.get_db_connection()
    
    @patch.object(MinIOLoader, 'get_db_connection')
    def test_create_bucket_new_bucket(self, mock_get_connection, minio_loader):
        """Test creating a new bucket."""
        # Mock client
        mock_client = Mock()
        mock_client.bucket_exists.return_value = False
        mock_get_connection.return_value = mock_client
        
        # Test bucket creation
        result = minio_loader.create_bucket('test-bucket')
        
        # Assertions
        assert result == Path('test-bucket')
        mock_client.bucket_exists.assert_called_once_with('test-bucket')
        mock_client.make_bucket.assert_called_once_with('test-bucket')
    
    @patch.object(MinIOLoader, 'get_db_connection')
    def test_create_bucket_existing_bucket(self, mock_get_connection, minio_loader):
        """Test handling existing bucket."""
        # Mock client
        mock_client = Mock()
        mock_client.bucket_exists.return_value = True
        mock_get_connection.return_value = mock_client
        
        # Test bucket creation
        result = minio_loader.create_bucket('existing-bucket')
        
        # Assertions
        assert result == Path('existing-bucket')
        mock_client.bucket_exists.assert_called_once_with('existing-bucket')
        mock_client.make_bucket.assert_not_called()
    
    @patch.object(MinIOLoader, 'get_db_connection')
    def test_create_bucket_failure(self, mock_get_connection, minio_loader):
        """Test bucket creation failure."""
        # Mock client
        mock_client = Mock()
        mock_client.bucket_exists.side_effect = Exception("Access denied")
        mock_get_connection.return_value = mock_client
        
        # Test should raise exception
        with pytest.raises(Exception, match="Access denied"):
            minio_loader.create_bucket('test-bucket')
    
    @patch('elt_pipeline.batch.utils.minio_loader.pq.write_table')
    @patch('elt_pipeline.batch.utils.minio_loader.pa')
    @patch.object(MinIOLoader, 'get_db_connection')
    def test_load_data_success(self, mock_get_connection, mock_pa, mock_write_table, 
                              minio_loader, sample_dataframe):
        """Test successful data loading to MinIO."""
        # Mock client and dependencies
        mock_client = Mock()
        mock_get_connection.return_value = mock_client
        
        mock_table = Mock()
        mock_pa.Table.from_pandas.return_value = mock_table
        
        # Test parameters
        params = {
            'bucket': 'test-bucket',
            'file_name': 'test-data.parquet',
            'file_format': 'parquet',
            'compression': 'snappy'
        }
        
        # Test data loading
        result = minio_loader.load_data(sample_dataframe, params)
        
        # Assertions
        assert result == len(sample_dataframe)
        mock_pa.Table.from_pandas.assert_called_once_with(sample_dataframe)
        mock_write_table.assert_called_once()
        mock_client.put_object.assert_called_once()
        
        # Verify put_object call arguments
        put_object_call = mock_client.put_object.call_args
        assert put_object_call[0][0] == 'test-bucket'  # bucket_name
        assert put_object_call[0][1] == 'test-data.parquet'  # file_name
        assert put_object_call[1]['content_type'] == 'application/parquet'
    
    @patch.object(MinIOLoader, 'get_db_connection')
    def test_load_data_empty_dataframe(self, mock_get_connection, minio_loader):
        """Test loading empty DataFrame."""
        mock_client = Mock()
        mock_get_connection.return_value = mock_client
        
        empty_df = pd.DataFrame()
        params = {
            'bucket': 'test-bucket',
            'file_name': 'empty-data.parquet'
        }
        
        # Test data loading
        result = minio_loader.load_data(empty_df, params)
        
        # Assertions
        assert result == 0
        mock_client.put_object.assert_not_called()
    
    @patch.object(MinIOLoader, 'get_db_connection')
    def test_load_data_none_dataframe(self, mock_get_connection, minio_loader):
        """Test loading None DataFrame."""
        mock_client = Mock()
        mock_get_connection.return_value = mock_client
        
        params = {
            'bucket': 'test-bucket',
            'file_name': 'none-data.parquet'
        }
        
        # Test data loading
        result = minio_loader.load_data(None, params)
        
        # Assertions
        assert result == 0
        mock_client.put_object.assert_not_called()
    
    @patch('elt_pipeline.batch.utils.minio_loader.pq.write_table')
    @patch('elt_pipeline.batch.utils.minio_loader.pa')
    @patch.object(MinIOLoader, 'get_db_connection')
    def test_load_data_with_default_params(self, mock_get_connection, 
                                          mock_pa, mock_write_table, minio_loader, sample_dataframe):
        """Test data loading with default parameters."""
        # Mock client
        mock_client = Mock()
        mock_get_connection.return_value = mock_client
        
        mock_table = Mock()
        mock_pa.Table.from_pandas.return_value = mock_table
        
        # Minimal parameters (should use defaults)
        params = {
            'bucket': 'test-bucket',
            'file_name': 'test-data.parquet'
        }
        
        # Test data loading
        result = minio_loader.load_data(sample_dataframe, params)
        
        # Assertions
        assert result == len(sample_dataframe)
        
        # Verify put_object call with default content type
        put_object_call = mock_client.put_object.call_args
        assert put_object_call[1]['content_type'] == 'application/parquet'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])