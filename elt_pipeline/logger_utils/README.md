# ELT Pipeline Logging System

This document describes the centralized logging system for the ELT pipeline, which provides unified, structured logging across all pipeline components.

## 🎯 Features

- **Unified Interface**: Consistent logging across all pipeline components
- **Specialized Loggers**: Pre-configured loggers for batch and streaming operations
- **Performance Tracking*```json
{
  "timestamp": "2025-01-15T10:30:15Z",
  "level": "INFO",
  "logger": "mysql_operations",
  "message": "Extraction complete: customers",
  "source": "mysql",
  "table": "customers",
  "records_extracted": 10000,
  "duration_seconds": 5.2,
  "operation_type": "extraction_complete"
}
```

## 🚀 Test Results

All logging system components have been validated:

```bash
$ uv run python elt_pipeline/logger_utils/test_logging_system.py

🎉 All logging tests completed successfully!
✅ Centralized logging system is working correctly
```

**Test Coverage:**
- ✅ Basic logging functionality
- ✅ Batch operations with metrics tracking
- ✅ Streaming operations with throughput monitoring
- ✅ All specialized loggers (MySQL, MinIO, Snowflake, dbt, event processing)
- ✅ Error handling and context preservation
- ✅ Performance logging and data quality tracking
- ✅ Environment awareness across dev/staging/production

## 🎯 Benefits

1. **Consistency**: Unified logging interface across all pipeline components
2. **Observability**: Rich metrics and structured data for monitoring
3. **Performance**: Built-in tracking for throughput, latency, and success rates
4. **Maintainability**: Centralized configuration and easy integration
5. **Production Ready**: Environment-aware, structured logging, log rotation
6. **Developer Experience**: Context managers, decorators, and pre-configured loggers

---

For more information or support, refer to the logger_utils source code or contact the development team.ics collection for data quality and performance
- **Environment Awareness**: Different logging behavior for dev/staging/production
- **Structured Logging**: JSON format support for production environments
- **Context Management**: Automatic operation tracking and error handling

## 📦 Components

### Core Architecture
- **`logger_config.py`**: Main logger class with enterprise features and environment-aware configuration
- **`batch_logger.py`**: Specialized logger for batch processing with built-in metrics tracking
- **`streaming_logger.py`**: Optimized for real-time processing with throughput and latency monitoring
- **`__init__.py`**: Clean package interface with comprehensive documentation
- **`test_logging_system.py`**: Complete test suite validating all functionality

### Key Classes
- **`ELTPipelineLogger`**: Core logger with performance tracking and structured logging
- **`BatchLogger`**: Tracks extraction/loading performance, data quality, and batch summaries
- **`StreamingLogger`**: Monitors events per second, latency, stream health, and real-time metrics
- **`LoggedOperation`**: Context manager for automatic operation lifecycle management
- **`BatchOperation`** & **`StreamingOperation`**: Specialized context managers with domain-specific logging

## ✅ Implementation Status

The logging system is **production-ready** and fully tested with:
- ✅ Environment-aware configuration (development/staging/production)
- ✅ Automatic performance and data quality tracking
- ✅ Context managers for operation lifecycle management
- ✅ Pre-configured specialized loggers for all pipeline components
- ✅ Comprehensive test coverage with successful validation
- ✅ Integration examples in existing pipeline files

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Specialized Loggers](#specialized-loggers)
- [Configuration](#configuration)
- [Examples](#examples)
- [Best Practices](#best-practices)
- [Migration Guide](#migration-guide)

## Overview

The centralized logging system provides:

- **Unified Interface**: Consistent logging across all pipeline components
- **Specialized Loggers**: Pre-configured loggers for batch and streaming operations
- **Performance Tracking**: Automatic metrics collection for data quality and performance
- **Environment Awareness**: Different logging behavior for dev/staging/production
- **Structured Logging**: JSON format support for production environments
- **Context Management**: Automatic operation tracking and error handling

## Quick Start

### Basic Usage

```python
from elt_pipeline.logger_utils import get_logger

logger = get_logger()
logger.info("Operation completed", records=1000, duration=2.5)
```

### Batch Operations

```python
from elt_pipeline.logger_utils import get_batch_logger, BatchOperation

logger = get_batch_logger()

# Simple logging
logger.info("Starting MySQL extraction", table="customers")

# With automatic performance tracking
with BatchOperation(logger, "extraction", source="mysql", table="customers") as op:
    # Your extraction logic here
    data = extract_customers()
    op.records_processed = len(data)
    # Automatic logging of duration and performance metrics
```

### Streaming Operations

```python
from elt_pipeline.logger_utils import get_streaming_logger, StreamingOperation

logger = get_streaming_logger()

# Simple event logging
logger.log_event_processed("order", processing_time_ms=15.2)

# With automatic throughput tracking
with StreamingOperation(logger, "process_event", event_type="order") as op:
    # Your event processing logic here
    process_order(event)
    # Automatic latency and throughput metrics
```

## Specialized Loggers

### BatchLogger

Optimized for batch processing operations with built-in metrics:

```python
from elt_pipeline.logger_utils import BatchLogger

logger = BatchLogger("data_pipeline")

# Track extraction performance
logger.log_extraction_complete(
    source="mysql",
    table="customers", 
    records_extracted=10000,
    duration=5.2
)

# Track data quality
logger.log_data_quality_check(
    table="customers",
    quality_results={"quality_score": 98.5, "null_count": 15}
)

# Get batch summary
summary = logger.log_batch_summary()
```

### StreamingLogger

Optimized for real-time processing with throughput monitoring:

```python
from elt_pipeline.logger_utils import StreamingLogger

logger = StreamingLogger("event_processor")

# Track individual events
logger.log_event_processed("order", processing_time_ms=15.2)

# Monitor throughput
logger.log_throughput_metrics(events_per_second=1250.5)

# Check stream health
health = logger.log_stream_health_check()
```

### Pre-configured Convenience Loggers

```python
from elt_pipeline.logger_utils import (
    get_mysql_logger,      # For MySQL operations
    get_minio_logger,      # For MinIO operations
    get_snowflake_logger,  # For Snowflake operations
    get_dbt_logger,        # For dbt operations
    get_event_processor_logger,  # For event processing
    get_data_ingestion_logger    # For data ingestion
)

mysql_logger = get_mysql_logger()
mysql_logger.log_extraction_start("mysql", "customers")
```

## Configuration

### Environment Variables

```bash
# Set environment (affects logging format and level)
export ENVIRONMENT=development  # development|staging|production

# Set log level (optional)
export LOG_LEVEL=INFO  # DEBUG|INFO|WARNING|ERROR|CRITICAL
```

### Environment Behaviors

| Environment | Format | Level | Output | Features |
|-------------|--------|-------|---------|----------|
| development | Human-readable | DEBUG | Console | Colorized, detailed |
| staging | Structured | INFO | Console + File | JSON format |
| production | JSON | WARNING | File with rotation | Minimal, structured |

## Examples

### Complete Batch Pipeline Example

```python
from elt_pipeline.logger_utils import get_mysql_logger, BatchOperation

def extract_mysql_data(table_name):
    logger = get_mysql_logger()
    
    with BatchOperation(logger, "extraction", source="mysql", table=table_name) as op:
        try:
            # Connect to database
            logger.debug("Connecting to MySQL", host=mysql_host)
            conn = mysql_connect()
            
            # Execute query
            query = f"SELECT * FROM {table_name}"
            logger.debug("Executing query", query=query)
            data = execute_query(conn, query)
            
            # Update metrics
            op.records_processed = len(data)
            
            logger.info("Extraction completed successfully",
                       table=table_name,
                       records=len(data))
            
            return data
            
        except Exception as e:
            logger.error("Extraction failed", 
                        table=table_name,
                        error=str(e),
                        error_type=type(e).__name__)
            raise
```

### Complete Streaming Pipeline Example

```python
from elt_pipeline.logger_utils import get_event_processor_logger, StreamingOperation

def process_event_stream():
    logger = get_event_processor_logger()
    
    logger.log_stream_start({"source": "kafka", "topic": "orders"})
    
    while True:
        try:
            event = get_next_event()
            
            with StreamingOperation(logger, "process_event", 
                                  event_type=event.type, 
                                  event_id=event.id) as op:
                
                # Process the event
                result = process_order_event(event)
                
                # Log additional context
                logger.debug("Event processed", 
                           event_id=event.id,
                           customer_id=event.customer_id,
                           amount=event.amount)
                
        except Exception as e:
            logger.error("Event processing failed",
                        event_id=event.id if 'event' in locals() else None,
                        error=str(e))
            
        # Periodic health checks
        if should_report_health():
            logger.log_stream_health_check()
```

## Best Practices

### 1. Use Appropriate Logger Types

```python
# ✅ Good: Use specialized loggers
mysql_logger = get_mysql_logger()
event_logger = get_event_processor_logger()

# ❌ Avoid: Generic logger for everything
generic_logger = get_logger()
```

### 2. Use Context Managers for Operations

```python
# ✅ Good: Automatic performance tracking
with BatchOperation(logger, "loading", destination="snowflake", table="orders") as op:
    load_data()
    op.records_processed = record_count

# ❌ Avoid: Manual start/end logging
logger.info("Starting loading")
start_time = time.time()
load_data()
logger.info(f"Loading completed in {time.time() - start_time}s")
```

### 3. Include Structured Context

```python
# ✅ Good: Structured, searchable logging
logger.info("Processing completed",
           table="customers",
           records_processed=1000,
           duration_seconds=2.5,
           data_quality_score=98.5)

# ❌ Avoid: String formatting only
logger.info(f"Processed 1000 records from customers in 2.5s")
```

### 4. Use Appropriate Log Levels

```python
# ✅ Good: Appropriate levels
logger.debug("SQL query", query=sql)          # Implementation details
logger.info("Operation completed", records=n)  # Important events
logger.warning("Quality below threshold", score=94)  # Potential issues
logger.error("Connection failed", error=str(e))      # Actual errors
```

### 5. Handle Exceptions Properly

```python
# ✅ Good: Structured error logging
try:
    process_data()
except DatabaseError as e:
    logger.error("Database operation failed",
                table=table_name,
                error=str(e),
                error_type="DatabaseError",
                operation="extraction")
    raise
```

## Migration Guide

### From Standard Python Logging

**Before:**
```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Processing {table_name}")
logger.info(f"Processed {count} records in {duration}s")
```

**After:**
```python
from elt_pipeline.logger_utils import get_batch_logger, BatchOperation

logger = get_batch_logger()

with BatchOperation(logger, "processing", table=table_name) as op:
    # Your logic here
    op.records_processed = count
```

### From Custom Logging

**Before:**
```python
def log_performance(operation, duration, records):
    print(f"PERF: {operation} took {duration}s for {records} records")

def extract_data():
    start = time.time()
    data = do_extraction()
    log_performance("extraction", time.time() - start, len(data))
```

**After:**
```python
from elt_pipeline.logger_utils import get_mysql_logger, BatchOperation

def extract_data():
    logger = get_mysql_logger()
    
    with BatchOperation(logger, "extraction", source="mysql") as op:
        data = do_extraction()
        op.records_processed = len(data)
        # Performance automatically logged
```

## Log Output Examples

### Development Environment
```
2024-01-15 10:30:15 | INFO | mysql_operations | 📤 Starting extraction: customers from mysql
2024-01-15 10:30:20 | INFO | mysql_operations | ✅ Extraction complete: customers (5.2s, 10000 records)
```

### Production Environment
```json
{
  "timestamp": "2024-01-15T10:30:15Z",
  "level": "INFO",
  "logger": "mysql_operations",
  "message": "Extraction complete: customers",
  "source": "mysql",
  "table": "customers",
  "records_extracted": 10000,
  "duration_seconds": 5.2,
  "operation_type": "extraction_complete"
}
```

---

For more information or support, refer to the logger_utils source code or contact the development team.