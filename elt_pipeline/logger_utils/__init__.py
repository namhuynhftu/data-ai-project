"""
ELT Pipeline Logger Utils

Centralized logging utilities for all ELT pipeline operations.

This package provides:
- Unified logging interface across all pipeline components
- Environment-aware configuration (development/staging/production)
- Automatic log organization by pipeline type
- Performance and data quality metrics logging
- Structured logging with JSON format support
- Context managers for operation tracking
- Specialized loggers for batch and streaming operations

Quick Start:
    from elt_pipeline.logger_utils import get_logger
    
    logger = get_logger()
    logger.info("Operation completed", records=1000, duration=2.5)

Specialized Loggers:
    # Batch operations with metrics
    from elt_pipeline.logger_utils import BatchLogger, BatchOperation
    
    logger = BatchLogger("data_pipeline")
    with BatchOperation(logger, "extraction", source="mysql", table="customers") as op:
        # Your extraction logic
        op.records_processed = 1000
    
    # Streaming operations with throughput tracking
    from elt_pipeline.logger_utils import StreamingLogger, StreamingOperation
    
    logger = StreamingLogger("event_stream")
    with StreamingOperation(logger, "process_event", event_type="order") as op:
        # Your event processing logic
        pass

Factory Functions:
    from elt_pipeline.logger_utils import (
        get_batch_logger,
        get_streaming_logger, 
        get_utils_logger,
        get_mysql_logger,      # Pre-configured for MySQL
        get_minio_logger,      # Pre-configured for MinIO
        get_snowflake_logger,  # Pre-configured for Snowflake
        get_event_processor_logger  # Pre-configured for event processing
    )

Operation Tracking:
    from elt_pipeline.logger_utils import LoggedOperation
    
    with LoggedOperation(logger, "data_extraction") as op:
        # Your operation code
        op.update_records(500)  # Track records processed

Function Decorators:
    from elt_pipeline.logger_utils import logged_function
    
    @logged_function("mysql_extraction", log_args=True)
    def extract_data(table_name):
        # Function automatically logged
        return data

Global Configuration:
    from elt_pipeline.logger_utils import configure_logging
    
    configure_logging(
        environment="production",
        log_level="WARNING",
        structured_logging=True
    )

Environment Variables:
    ENVIRONMENT: development|staging|production (default: development)
    LOG_LEVEL: DEBUG|INFO|WARNING|ERROR|CRITICAL (default: INFO)
    
Log Organization:
    logs/
    ├── batch/          # Batch pipeline logs
    ├── streaming/      # Streaming pipeline logs  
    ├── utils/          # Utility operation logs
    ├── pipeline/       # Main pipeline logs
    └── errors/         # Centralized error logs
"""

# Import main logging components
from .logger_config import (
    # Main logger class
    ELTPipelineLogger,
    
    # Factory functions
    get_logger,
    get_batch_logger,
    get_streaming_logger,
    get_utils_logger,
    
    # Context managers and decorators
    LoggedOperation,
    logged_function
)

# Import specialized batch logging
from .batch_logger import (
    BatchLogger,
    BatchOperation,
    get_mysql_logger,
    get_minio_logger,
    get_snowflake_logger,
    get_dbt_logger
)

# Import specialized streaming logging
from .streaming_logger import (
    StreamingLogger,
    StreamingOperation,
    get_event_processor_logger,
    get_data_ingestion_logger,
    get_stream_processor_logger,
    get_realtime_loader_logger
)

# Package metadata
__version__ = "1.1.0"
__author__ = "ELT Pipeline Team"
__description__ = "Centralized logging utilities for ELT pipeline operations"

# Public API
__all__ = [
    # Core classes
    "ELTPipelineLogger",
    
    # Factory functions
    "get_logger",
    "get_batch_logger", 
    "get_streaming_logger",
    "get_utils_logger",
    
    # Specialized batch logging
    "BatchLogger",
    "BatchOperation",
    "get_mysql_logger",
    "get_minio_logger", 
    "get_snowflake_logger",
    "get_dbt_logger",
    
    # Specialized streaming logging
    "StreamingLogger",
    "StreamingOperation",
    "get_event_processor_logger",
    "get_data_ingestion_logger",
    "get_stream_processor_logger",
    "get_realtime_loader_logger",
    
    # Context managers and decorators
    "LoggedOperation",
    "logged_function",
]

# Initialize default configuration on import
import os

# Set default environment if not specified
if "ENVIRONMENT" not in os.environ:
    os.environ["ENVIRONMENT"] = "development"

# Set default log level if not specified  
if "LOG_LEVEL" not in os.environ:
    os.environ["LOG_LEVEL"] = "INFO"