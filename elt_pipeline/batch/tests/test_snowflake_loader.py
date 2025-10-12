#!/usr/bin/env python3
"""
Unit tests for SnowflakeLoader class.

Tests all methods of SnowflakeLoader including connection management,
data loading, and error handling scenarios.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import snowflake.connector
from pathlib import Path
import sys

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from elt_pipeline.batch.utils.snowflake_loader import SnowflakeLoader


class TestSnowflakeLoader:
    """Test cases for SnowflakeLoader class."""
    
    @pytest.fixture
    def snowflake_config(self):
        """Sample Snowflake configuration for testing."""
        return {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "warehouse": "test_warehouse",
            "database": "TEST_DB",
            "schema": "RAW_DATA",
            "role": "test_role"
        }
    
    @pytest.fixture
    def sample_dataframe(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'city': ['New York', 'London', 'Tokyo']
        })
    
    @pytest.fixture
    def snowflake_loader(self, snowflake_config):
        """Create SnowflakeLoader instance for testing."""
        return SnowflakeLoader(snowflake_config)
    
    def test_snowflake_loader_initialization(self, snowflake_config):
        """Test SnowflakeLoader initialization with configuration."""
        loader = SnowflakeLoader(snowflake_config)
        
        assert loader.account == "test_account"
        assert loader.user == "test_user"
        assert loader.password == "test_password"
        assert loader.warehouse == "test_warehouse"
        assert loader.database == "TEST_DB"
        assert loader.schema == "RAW_DATA"
        assert loader.role == "test_role"
        assert loader.connection is None
    
    def test_snowflake_loader_initialization_missing_required_config(self):
        """Test SnowflakeLoader initialization with missing required configuration."""
        incomplete_config = {
            "account": "test_account",
            "user": "test_user"
            # Missing password, warehouse, database, schema, role
        }
        
        with pytest.raises(KeyError):
            SnowflakeLoader(incomplete_config)
    
    @patch('snowflake.connector.connect')
    def test_get_db_connection_success(self, mock_connect, snowflake_loader):
        """Test successful database connection."""
        # Mock successful connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Test connection
        connection = snowflake_loader.get_db_connection()
        
        # Assertions
        assert connection == mock_connection
        assert snowflake_loader.connection == mock_connection
        
        # Verify connection parameters
        mock_connect.assert_called_once_with(
            account="test_account",
            user="test_user",
            password="test_password",
            warehouse="test_warehouse",
            database="TEST_DB",
            schema="RAW_DATA",
            role="test_role"
        )
    
    @patch('snowflake.connector.connect')
    def test_get_db_connection_failure(self, mock_connect, snowflake_loader):
        """Test database connection failure."""
        # Mock connection failure
        mock_connect.side_effect = snowflake.connector.errors.DatabaseError("Connection failed")
        
        # Test connection failure
        with pytest.raises(snowflake.connector.errors.DatabaseError):
            snowflake_loader.get_db_connection()
        
        # Verify connection is None after failure
        assert snowflake_loader.connection is None
    
    @patch('snowflake.connector.connect')
    def test_get_db_connection_reuse_existing(self, mock_connect, snowflake_loader):
        """Test reusing existing database connection."""
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # First connection
        connection1 = snowflake_loader.get_db_connection()
        
        # Second connection should reuse existing
        connection2 = snowflake_loader.get_db_connection()
        
        # Assertions
        assert connection1 == connection2
        assert connection1 == mock_connection
        
        # Verify connect is called only once
        mock_connect.assert_called_once()
    
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('snowflake.connector.connect')
    def test_load_data_success(self, mock_connect, mock_write_pandas, 
                              snowflake_loader, sample_dataframe):
        """Test successful data loading to Snowflake."""
        # Mock connection and write_pandas
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_write_pandas.return_value = (True, 1, len(sample_dataframe), None)
        
        # Test data loading
        result = snowflake_loader.load_data(sample_dataframe, "test_table")
        
        # Assertions
        assert result == len(sample_dataframe)
        
        # Verify write_pandas was called correctly
        mock_write_pandas.assert_called_once_with(
            mock_connection,
            sample_dataframe,
            "test_table".upper(),  # Snowflake converts to uppercase
            database="TEST_DB",
            schema="RAW_DATA",
            quote_identifiers=False,
            auto_create_table=True
        )
    
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('snowflake.connector.connect')
    def test_load_data_write_failure(self, mock_connect, mock_write_pandas, 
                                    snowflake_loader, sample_dataframe):
        """Test data loading failure during write operation."""
        # Mock connection and write_pandas failure
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_write_pandas.return_value = (False, 0, 0, "Write operation failed")
        
        # Test data loading failure
        with pytest.raises(Exception) as exc_info:
            snowflake_loader.load_data(sample_dataframe, "test_table")
        
        assert "Failed to load data to Snowflake" in str(exc_info.value)
    
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('snowflake.connector.connect')
    def test_load_data_exception_during_write(self, mock_connect, mock_write_pandas, 
                                             snowflake_loader, sample_dataframe):
        """Test data loading with exception during write operation."""
        # Mock connection and write_pandas exception
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_write_pandas.side_effect = Exception("Database connection lost")
        
        # Test data loading with exception
        with pytest.raises(Exception) as exc_info:
            snowflake_loader.load_data(sample_dataframe, "test_table")
        
        assert "Database connection lost" in str(exc_info.value)
    
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('snowflake.connector.connect')
    def test_load_data_empty_dataframe(self, mock_connect, mock_write_pandas, 
                                      snowflake_loader):
        """Test loading empty DataFrame."""
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_write_pandas.return_value = (True, 1, 0, None)
        
        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        result = snowflake_loader.load_data(empty_df, "test_table")
        
        # Assertions
        assert result == 0
        mock_write_pandas.assert_called_once()
    
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('snowflake.connector.connect')
    def test_load_data_with_schema_override(self, mock_connect, mock_write_pandas, 
                                           snowflake_loader, sample_dataframe):
        """Test data loading with schema override."""
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_write_pandas.return_value = (True, 1, len(sample_dataframe), None)
        
        # Test with schema override
        result = snowflake_loader.load_data(
            sample_dataframe, 
            "test_table", 
            schema="CUSTOM_SCHEMA"
        )
        
        # Assertions
        assert result == len(sample_dataframe)
        
        # Verify schema override was used
        mock_write_pandas.assert_called_once_with(
            mock_connection,
            sample_dataframe,
            "TEST_TABLE",
            database="TEST_DB",
            schema="CUSTOM_SCHEMA",
            quote_identifiers=False,
            auto_create_table=True
        )
    
    def test_close_connection_with_active_connection(self, snowflake_loader):
        """Test closing active database connection."""
        # Mock active connection
        mock_connection = Mock()
        snowflake_loader.connection = mock_connection
        
        # Test connection closure
        snowflake_loader.close_connection()
        
        # Assertions
        mock_connection.close.assert_called_once()
        assert snowflake_loader.connection is None
    
    def test_close_connection_without_active_connection(self, snowflake_loader):
        """Test closing connection when no active connection exists."""
        # Ensure no active connection
        snowflake_loader.connection = None
        
        # Test connection closure (should not raise exception)
        snowflake_loader.close_connection()
        
        # Assertions
        assert snowflake_loader.connection is None
    
    @patch('snowflake.connector.connect')
    def test_context_manager_success(self, mock_connect, snowflake_loader, sample_dataframe):
        """Test SnowflakeLoader as context manager with successful operations."""
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Test context manager
        with snowflake_loader as loader:
            assert loader.connection == mock_connection
            assert loader == snowflake_loader
        
        # Verify connection is closed
        mock_connection.close.assert_called_once()
        assert snowflake_loader.connection is None
    
    @patch('snowflake.connector.connect')
    def test_context_manager_with_exception(self, mock_connect, snowflake_loader):
        """Test SnowflakeLoader context manager when exception occurs."""
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Test context manager with exception
        with pytest.raises(ValueError):
            with snowflake_loader as loader:
                raise ValueError("Test exception")
        
        # Verify connection is still closed even with exception
        mock_connection.close.assert_called_once()
        assert snowflake_loader.connection is None
    
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('snowflake.connector.connect')
    def test_load_data_table_name_case_handling(self, mock_connect, mock_write_pandas, 
                                                snowflake_loader, sample_dataframe):
        """Test table name case handling (Snowflake converts to uppercase)."""
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_write_pandas.return_value = (True, 1, len(sample_dataframe), None)
        
        # Test with lowercase table name
        snowflake_loader.load_data(sample_dataframe, "my_test_table")
        
        # Verify table name is converted to uppercase
        call_args = mock_write_pandas.call_args[1]
        assert call_args['table_name'] == "MY_TEST_TABLE"
    
    @patch('snowflake.connector.pandas_tools.write_pandas')
    @patch('snowflake.connector.connect')
    def test_load_data_auto_create_table_parameter(self, mock_connect, mock_write_pandas, 
                                                  snowflake_loader, sample_dataframe):
        """Test auto_create_table parameter is properly set."""
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_write_pandas.return_value = (True, 1, len(sample_dataframe), None)
        
        # Test data loading
        snowflake_loader.load_data(sample_dataframe, "test_table")
        
        # Verify auto_create_table is True
        call_args = mock_write_pandas.call_args[1]
        assert call_args['auto_create_table'] is True
    
    def test_repr_method(self, snowflake_loader):
        """Test string representation of SnowflakeLoader."""
        repr_str = repr(snowflake_loader)
        
        assert "SnowflakeLoader" in repr_str
        assert "test_account" in repr_str
        assert "test_user" in repr_str
        assert "TEST_DB" in repr_str
        assert "RAW_DATA" in repr_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])