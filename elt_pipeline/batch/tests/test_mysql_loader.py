#!/usr/bin/env python3
"""
Unit tests for MySQL loader functionality.

Tests the MySQLLoader class methods including connection handling,
data extraction, and watermark functionality.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from elt_pipeline.batch.utils.mysql_loader import MySQLLoader


class TestMySQLLoader:
    """Test cases for MySQLLoader class."""
    
    @pytest.fixture
    def mysql_params(self):
        """Sample MySQL connection parameters for testing."""
        return {
            'host': 'localhost',
            'port': 3306,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
    
    @pytest.fixture
    def mysql_loader(self, mysql_params):
        """Create MySQLLoader instance for testing."""
        return MySQLLoader(mysql_params)
    
    def test_init(self, mysql_loader, mysql_params):
        """Test MySQLLoader initialization."""
        assert mysql_loader.params == mysql_params
        assert mysql_loader._engine is None
        assert hasattr(mysql_loader, 'logger')
    
    @patch('elt_pipeline.batch.utils.mysql_loader.create_engine')
    def test_get_db_connection_success(self, mock_create_engine, mysql_loader):
        """Test successful database connection."""
        # Mock engine with context manager support
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        mock_create_engine.return_value = mock_engine
        
        # Test connection
        result = mysql_loader.get_db_connection()
        
        # Assertions
        assert result == mock_engine
        assert mysql_loader._engine == mock_engine
        mock_create_engine.assert_called_once()
        
        # Verify connection string format
        call_args = mock_create_engine.call_args[0][0]
        assert 'mysql+pymysql://' in call_args
        assert 'test_user' in call_args
        assert 'localhost:3306' in call_args
        assert 'test_db' in call_args
    
    @patch('elt_pipeline.batch.utils.mysql_loader.create_engine')
    def test_get_db_connection_failure(self, mock_create_engine, mysql_loader):
        """Test database connection failure."""
        mock_create_engine.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            mysql_loader.get_db_connection()
    
    @patch('elt_pipeline.batch.utils.mysql_loader.pymysql.connect')
    @patch('pandas.read_sql')
    def test_extract_data_success(self, mock_read_sql, mock_connect, mysql_loader):
        """Test successful data extraction."""
        # Mock data
        expected_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        mock_read_sql.return_value = expected_df
        
        # Mock connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Test extraction
        sql = "SELECT * FROM users"
        result = mysql_loader.extract_data(sql)
        
        # Assertions
        pd.testing.assert_frame_equal(result, expected_df)
        mock_connect.assert_called_once()
        mock_read_sql.assert_called_once_with(sql, con=mock_connection)
        mock_connection.close.assert_called_once()
    
    @patch('elt_pipeline.batch.utils.mysql_loader.pymysql.connect')
    def test_extract_data_failure(self, mock_connect, mysql_loader):
        """Test data extraction failure."""
        mock_connect.side_effect = Exception("SQL execution failed")
        
        with pytest.raises(Exception, match="SQL execution failed"):
            mysql_loader.extract_data("SELECT * FROM users")
    
    @patch.object(MySQLLoader, 'get_db_connection')
    def test_get_watermark_success(self, mock_get_connection, mysql_loader):
        """Test successful watermark retrieval."""
        # Mock engine and result
        mock_engine = Mock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.__getitem__ = MagicMock(return_value='2023-12-01 10:00:00')
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        mock_connection.execute.return_value.fetchone.return_value = mock_result
        mock_get_connection.return_value = mock_engine
        
        # Test watermark retrieval
        result = mysql_loader.get_watermark('orders', 'created_at')
        
        # Assertions
        assert result == '2023-12-01 10:00:00'
        mock_connection.execute.assert_called_once()
    
    @patch.object(MySQLLoader, 'get_db_connection')
    def test_get_watermark_no_result(self, mock_get_connection, mysql_loader):
        """Test watermark retrieval with no result."""
        # Mock engine with no result
        mock_engine = Mock()
        mock_connection = MagicMock()
        
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        mock_connection.execute.return_value.fetchone.return_value = None
        mock_get_connection.return_value = mock_engine
        
        # Test watermark retrieval
        result = mysql_loader.get_watermark('orders', 'created_at')
        
        # Assertions
        assert result is None
    
    @patch.object(MySQLLoader, 'get_db_connection')
    def test_get_watermark_failure(self, mock_get_connection, mysql_loader):
        """Test watermark retrieval failure."""
        mock_get_connection.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            mysql_loader.get_watermark('orders', 'created_at')
    
    def test_context_manager(self, mysql_loader):
        """Test MySQLLoader as context manager."""
        with patch.object(mysql_loader, 'get_db_connection') as mock_get_conn:
            mock_engine = Mock()
            mock_get_conn.return_value = mock_engine
            mysql_loader._engine = mock_engine  # Set the engine attribute
            
            # Test context manager entry
            with mysql_loader as loader:
                assert loader == mysql_loader
                mock_get_conn.assert_called_once()
            
            # Test context manager exit
            mock_engine.dispose.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])