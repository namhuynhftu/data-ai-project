#!/usr/bin/env python3
"""
Test script to verify the batch pipeline logging integration.

This script tests that all the batch pipeline files can be imported
and that the logging system is properly integrated.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test that all batch pipeline modules can be imported successfully."""
    print("🧪 Testing batch pipeline imports...")
    
    try:
        # Test ops imports
        from elt_pipeline.batch.ops.extract_data_from_mysql import extract_data_from_mysql, load_run_config
        from elt_pipeline.batch.ops.load_data_to_minio import load_data_to_minio
        from elt_pipeline.batch.ops.load_data_to_snowflake import load_data_to_snowflake
        
        # Test utils imports
        from elt_pipeline.batch.utils.mysql_loader import MySQLLoader
        from elt_pipeline.batch.utils.minio_loader import MinIOLoader
        from elt_pipeline.batch.utils.snowflake_loader import SnowflakeLoader
        
        print("✅ All batch pipeline imports successful")
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_logger_integration():
    """Test that loggers are properly integrated."""
    print("🧪 Testing logger integration...")
    
    try:
        from elt_pipeline.batch.utils.mysql_loader import MySQLLoader
        from elt_pipeline.batch.utils.minio_loader import MinIOLoader
        from elt_pipeline.batch.utils.snowflake_loader import SnowflakeLoader
        
        # Test that loggers are properly initialized
        test_params = {
            'host': 'localhost',
            'port': 3306,
            'user': 'test',
            'password': 'test',
            'database': 'test'
        }
        
        mysql_loader = MySQLLoader(test_params)
        assert hasattr(mysql_loader, 'logger'), "MySQLLoader should have logger attribute"
        
        minio_params = {
            'endpoint': 'localhost:9000',
            'access_key': 'test',
            'secret_key': 'test'
        }
        
        minio_loader = MinIOLoader(minio_params)
        assert hasattr(minio_loader, 'logger'), "MinIOLoader should have logger attribute"
        
        snowflake_loader = SnowflakeLoader(test_params)
        assert hasattr(snowflake_loader, 'logger'), "SnowflakeLoader should have logger attribute"
        
        print("✅ Logger integration test passed")
        return True
        
    except Exception as e:
        print(f"❌ Logger integration test failed: {e}")
        return False

def test_logging_functionality():
    """Test that logging functionality works correctly."""
    print("🧪 Testing logging functionality...")
    
    try:
        from elt_pipeline.logger_utils import get_mysql_logger, get_minio_logger, get_snowflake_logger
        
        # Test that specialized loggers can be created
        mysql_logger = get_mysql_logger()
        minio_logger = get_minio_logger()
        snowflake_logger = get_snowflake_logger()
        
        # Test that loggers have the expected methods
        for logger in [mysql_logger, minio_logger, snowflake_logger]:
            assert hasattr(logger, 'info'), "Logger should have info method"
            assert hasattr(logger, 'error'), "Logger should have error method"
            assert hasattr(logger, 'debug'), "Logger should have debug method"
            assert hasattr(logger, 'warning'), "Logger should have warning method"
        
        # Test basic logging
        mysql_logger.info("Test MySQL logging", test=True)
        minio_logger.info("Test MinIO logging", test=True) 
        snowflake_logger.info("Test Snowflake logging", test=True)
        
        print("✅ Logging functionality test passed")
        return True
        
    except Exception as e:
        print(f"❌ Logging functionality test failed: {e}")
        return False

def main():
    """Run all batch pipeline integration tests."""
    print("🚀 Starting Batch Pipeline Logging Integration Tests")
    print("=" * 60)
    
    all_passed = True
    
    # Run tests
    tests = [
        test_imports,
        test_logger_integration,
        test_logging_functionality
    ]
    
    for test in tests:
        if not test():
            all_passed = False
        print()
    
    print("=" * 60)
    if all_passed:
        print("🎉 All batch pipeline integration tests passed!")
        print("✅ Centralized logging is properly integrated into batch pipeline")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()