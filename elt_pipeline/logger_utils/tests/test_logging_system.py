#!/usr/bin/env python3
"""
Test script for the centralized logging system.

This script demonstrates and tests all features of the ELT pipeline 
logging utilities to ensure they work correctly.
"""

import time
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def test_basic_logging():
    """Test basic logging functionality."""
    print("🧪 Testing basic logging...")
    
    from elt_pipeline.logger_utils import get_logger
    
    logger = get_logger()
    logger.info("Basic logging test", test_type="basic", status="success")
    logger.debug("Debug message", detail="This should show in development")
    logger.warning("Warning message", alert_level="medium")
    
    print("✅ Basic logging test completed")


def test_batch_logging():
    """Test batch logging functionality."""
    print("🧪 Testing batch logging...")
    
    from elt_pipeline.logger_utils import get_mysql_logger, BatchOperation
    
    logger = get_mysql_logger()
    
    # Test extraction logging
    with BatchOperation(logger, "extraction", source="mysql", table="customers") as op:
        time.sleep(0.1)  # Simulate work
        op.records_processed = 1000
    
    # Test direct batch methods
    logger.log_extraction_complete("mysql", "orders", 5000, 2.5)
    logger.log_data_quality_check("orders", {"quality_score": 98.5, "null_count": 15})
    
    # Test batch summary
    summary = logger.log_batch_summary()
    print(f"   Batch summary: {summary}")
    
    print("✅ Batch logging test completed")


def test_streaming_logging():
    """Test streaming logging functionality."""
    print("🧪 Testing streaming logging...")
    
    from elt_pipeline.logger_utils import get_event_processor_logger, StreamingOperation
    
    logger = get_event_processor_logger()
    
    # Test stream start
    logger.log_stream_start({"source": "kafka", "topic": "orders"})
    
    # Test event processing
    for i in range(5):
        with StreamingOperation(logger, "process_event", 
                              event_type="order", 
                              event_id=f"order_{i}") as op:
            time.sleep(0.02)  # Simulate processing
    
    # Test throughput metrics
    logger.log_throughput_metrics(1250.5, 60)
    logger.log_latency_metrics(15.2, 25.0, 45.0)
    
    # Test health check
    health = logger.log_stream_health_check()
    print(f"   Stream health: {health}")
    
    print("✅ Streaming logging test completed")


def test_specialized_loggers():
    """Test all specialized logger types."""
    print("🧪 Testing specialized loggers...")
    
    from elt_pipeline.logger_utils import (
        get_minio_logger,
        get_snowflake_logger,
        get_dbt_logger,
        get_data_ingestion_logger,
        get_realtime_loader_logger
    )
    
    # Test each specialized logger
    loggers = [
        ("MinIO", get_minio_logger()),
        ("Snowflake", get_snowflake_logger()),
        ("dbt", get_dbt_logger()),
        ("Data Ingestion", get_data_ingestion_logger()),
        ("Realtime Loader", get_realtime_loader_logger())
    ]
    
    for name, logger in loggers:
        logger.info(f"{name} logger test", logger_type=name, status="working")
    
    print("✅ Specialized loggers test completed")


def test_error_handling():
    """Test error handling and logging."""
    print("🧪 Testing error handling...")
    
    from elt_pipeline.logger_utils import BatchLogger, BatchOperation
    
    logger = BatchLogger("test_operations")
    
    try:
        with BatchOperation(logger, "test_operation", table="test") as op:
            op.records_processed = 100
            raise ValueError("Test error for logging demonstration")
    except ValueError:
        logger.info("Error handling test completed", expected_error=True)
    
    print("✅ Error handling test completed")


def test_performance_logging():
    """Test performance logging features."""
    print("🧪 Testing performance logging...")
    
    from elt_pipeline.logger_utils import get_batch_logger
    
    logger = get_batch_logger()
    
    # Test manual performance logging using logger methods
    logger.log_performance("test_operation", 2.5, 1000)
    
    # Test data quality logging using logger methods
    logger.log_data_quality("test_table", 1000, 
                           null_records=5,
                           duplicate_records=2,
                           quality_score=99.3)
    
    print("✅ Performance logging test completed")


def test_environment_awareness():
    """Test environment-aware logging behavior."""
    print("🧪 Testing environment awareness...")
    
    # Save original environment
    original_env = os.environ.get("ENVIRONMENT")
    
    try:
        # Test different environments
        for env in ["development", "staging", "production"]:
            os.environ["ENVIRONMENT"] = env
            print(f"   Testing environment: {env}")
            
            from elt_pipeline.logger_utils import get_logger
            logger = get_logger()
            logger.info(f"Environment test: {env}", 
                       current_environment=env,
                       test_type="environment_awareness")
            
            # Force module reload for next environment test
            import importlib
            import elt_pipeline.logger_utils.logger_config
            importlib.reload(elt_pipeline.logger_utils.logger_config)
            
    finally:
        # Restore original environment
        if original_env:
            os.environ["ENVIRONMENT"] = original_env
        elif "ENVIRONMENT" in os.environ:
            del os.environ["ENVIRONMENT"]
    
    print("✅ Environment awareness test completed")


def main():
    """Run all logging tests."""
    print("🚀 Starting ELT Pipeline Logging System Tests")
    print("=" * 60)
    
    try:
        test_basic_logging()
        print()
        
        test_batch_logging()
        print()
        
        test_streaming_logging()
        print()
        
        test_specialized_loggers()
        print()
        
        test_error_handling()
        print()
        
        test_performance_logging()
        print()
        
        test_environment_awareness()
        print()
        
        print("=" * 60)
        print("🎉 All logging tests completed successfully!")
        print("✅ Centralized logging system is working correctly")
        
    except Exception as e:
        print("=" * 60)
        print(f"❌ Test failed with error: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()