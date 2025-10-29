#!/usr/bin/env python3
"""
ELT Pipeline Logger Configuration

Centralized logging utility for all ELT pipeline operations.
Provides consistent logging across batch, streaming, and utility components.

Features:
- Unified logging interface for all pipeline components
- Environment-aware configuration (dev/staging/prod)
- Automatic log organization by pipeline type
- Performance and data quality metrics logging
- Structured logging with JSON format support
- Automatic log rotation and error aggregation
- Context managers for operation tracking

Usage:
    from elt_pipeline.logger_utils.logger_config import get_logger
    
    logger = get_logger()
    logger.info("Operation completed", records=1000, duration=2.5)
"""

import logging
import logging.handlers
import os
import sys
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Union
from functools import wraps
import inspect


class ELTPipelineLogger:
    """
    Main logger class for ELT pipeline operations.
    
    Provides unified logging interface with automatic configuration,
    performance tracking, and structured logging capabilities.
    """
    
    # Class-level cache for logger instances to avoid duplicates
    _logger_cache = {}
    
    def __init__(
        self,
        name: str,
        pipeline_type: str = None,
        log_level: str = None,
        enable_file_logging: bool = True,
        enable_console_logging: bool = True,
        enable_rotation: bool = True,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        structured_logging: bool = None
    ):
        """
        Initialize ELT Pipeline logger.
        
        Args:
            name: Logger name (usually module __name__)
            pipeline_type: Type of pipeline (batch, streaming, utils, etc.)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            enable_file_logging: Enable file logging
            enable_console_logging: Enable console logging
            enable_rotation: Enable log file rotation
            max_bytes: Maximum log file size before rotation
            backup_count: Number of backup files to keep
            structured_logging: Use JSON structured logging format
        """
        self.name = name
        self.pipeline_type = pipeline_type or self._detect_pipeline_type(name)
        
        # Environment-aware configuration
        self.environment = os.getenv("ENVIRONMENT", "development").lower()
        self.log_level = self._get_log_level(log_level)
        self.structured_logging = self._get_structured_logging_setting(structured_logging)
        
        # Create logs directory structure
        self._setup_log_directories()
        
        # Initialize performance tracking
        self.metrics = {
            "start_time": datetime.now().isoformat(),
            "operations": [],
            "total_records_processed": 0,
            "total_errors": 0,
            "total_warnings": 0
        }
        
        # Setup the actual logger
        self.logger = self._setup_logger(
            enable_file_logging,
            enable_console_logging,
            enable_rotation,
            max_bytes,
            backup_count
        )
        
        # Log initialization
        self._log_initialization()
    
    def _detect_pipeline_type(self, name: str) -> str:
        """Auto-detect pipeline type from logger name."""
        name_lower = name.lower()
        
        if "batch" in name_lower:
            return "batch"
        elif "streaming" in name_lower:
            return "streaming"
        elif "utils" in name_lower or "util" in name_lower:
            return "utils"
        elif any(keyword in name_lower for keyword in ["mysql", "snowflake", "minio", "psql"]):
            return "utils"
        elif "pipeline" in name_lower:
            return "pipeline"
        elif "test" in name_lower:
            return "tests"
        else:
            return "general"
    
    def _get_log_level(self, log_level: str) -> int:
        """Get log level with environment-aware defaults."""
        if log_level:
            return getattr(logging, log_level.upper())
        
        # Environment-specific defaults
        env_log_level = os.getenv("LOG_LEVEL")
        if env_log_level:
            return getattr(logging, env_log_level.upper())
        
        # Default levels by environment
        if self.environment == "production":
            return logging.WARNING
        elif self.environment == "staging":
            return logging.INFO
        else:  # development
            return logging.DEBUG
    
    def _get_structured_logging_setting(self, structured_logging: bool) -> bool:
        """Get structured logging setting with environment awareness."""
        if structured_logging is not None:
            return structured_logging
        
        # Enable structured logging in production
        return self.environment == "production"
    
    def _setup_log_directories(self):
        """Create log directory structure."""
        self.logs_base_dir = Path("logs")
        self.pipeline_logs_dir = self.logs_base_dir / self.pipeline_type
        self.error_logs_dir = self.logs_base_dir / "errors"
        
        # Create directories
        for directory in [self.logs_base_dir, self.pipeline_logs_dir, self.error_logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _get_log_filename(self) -> Path:
        """Generate log filename with timestamp."""
        # Clean name for filename
        clean_name = self.name.replace("elt_pipeline.", "").replace(".", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.pipeline_logs_dir / f"{clean_name}_{timestamp}.log"
    
    def _get_log_format(self) -> str:
        """Get log format string based on logging type."""
        if self.structured_logging:
            return "%(message)s"  # JSON messages only
        else:
            return (
                "%(asctime)s - %(name)s - %(levelname)s - "
                "[PID:%(process)d] - %(funcName)s:%(lineno)d - %(message)s"
            )
    
    def _setup_logger(
        self,
        enable_file_logging: bool,
        enable_console_logging: bool,
        enable_rotation: bool,
        max_bytes: int,
        backup_count: int
    ) -> logging.Logger:
        """Setup and configure logger with appropriate handlers."""
        
        # Create or get existing logger
        logger = logging.getLogger(self.name)
        
        # Clear existing handlers to avoid duplicates
        logger.handlers.clear()
        logger.setLevel(self.log_level)
        
        # Prevent propagation to root logger
        logger.propagate = False
        
        # Create formatter
        formatter = logging.Formatter(self._get_log_format())
        
        # File handler for main logs
        if enable_file_logging:
            log_file = self._get_log_filename()
            
            if enable_rotation:
                file_handler = logging.handlers.RotatingFileHandler(
                    log_file,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding='utf-8'
                )
            else:
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
            
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            self.log_file_path = log_file
        
        # Console handler with optional colors
        if enable_console_logging:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            
            # Try to use colored output for better readability
            try:
                import colorlog
                color_formatter = colorlog.ColoredFormatter(
                    "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    log_colors={
                        'DEBUG': 'cyan',
                        'INFO': 'green',
                        'WARNING': 'yellow',
                        'ERROR': 'red',
                        'CRITICAL': 'red,bg_white',
                    }
                )
                console_handler.setFormatter(color_formatter)
            except ImportError:
                # Fallback to standard formatter if colorlog not available
                console_handler.setFormatter(formatter)
            
            logger.addHandler(console_handler)
        
        # Error file handler (always enabled for ERROR and CRITICAL)
        error_log_file = self.error_logs_dir / f"{self.pipeline_type}_errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)
        
        return logger
    
    def _log_initialization(self):
        """Log logger initialization details."""
        init_info = {
            "logger_name": self.name,
            "pipeline_type": self.pipeline_type,
            "environment": self.environment,
            "log_level": logging.getLevelName(self.log_level),
            "structured_logging": self.structured_logging
        }
        
        if hasattr(self, 'log_file_path'):
            init_info["log_file"] = str(self.log_file_path)
        
        self._log_message("info", "🚀 ELT Pipeline Logger initialized", **init_info)
    
    def _log_message(self, level: str, message: str, **kwargs):
        """Internal method to log messages with optional structured data."""
        log_method = getattr(self.logger, level)
        
        if self.structured_logging and kwargs:
            # Create structured log entry
            log_data = {
                "timestamp": datetime.now().isoformat(),
                "level": level.upper(),
                "pipeline_type": self.pipeline_type,
                "logger_name": self.name,
                "message": message,
                **kwargs
            }
            log_method(json.dumps(log_data, default=str))
        else:
            # Standard log format with key-value pairs
            if kwargs:
                extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
                full_message = f"{message} | {extra_info}"
            else:
                full_message = message
            
            log_method(full_message)
    
    # Public logging methods
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self._log_message("debug", message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log_message("info", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self._log_message("warning", message, **kwargs)
        self.metrics["total_warnings"] += 1
    
    def error(self, message: str, **kwargs):
        """Log error message."""
        self._log_message("error", message, **kwargs)
        self.metrics["total_errors"] += 1
    
    def critical(self, message: str, **kwargs):
        """Log critical message."""
        self._log_message("critical", message, **kwargs)
        self.metrics["total_errors"] += 1
    
    # Specialized logging methods for data pipeline operations
    def log_performance(self, operation: str, duration: float, records: int = 0, **kwargs):
        """Log performance metrics for operations."""
        rate = records / duration if duration > 0 else 0
        
        perf_data = {
            "operation": operation,
            "duration_seconds": round(duration, 2),
            "records_processed": records,
            "records_per_second": round(rate, 2),
            **kwargs
        }
        
        # Track in metrics
        self.metrics["operations"].append(perf_data)
        self.metrics["total_records_processed"] += records
        
        self.info(f"Performance: {operation}", **perf_data)
    
    def log_database_operation(self, operation: str, table: str, rows_affected: int, 
                              duration: float, query: str = None, **kwargs):
        """Log database operations with performance metrics."""
        db_data = {
            "operation": operation,
            "table": table,
            "rows_affected": rows_affected,
            "duration_seconds": round(duration, 2),
            **kwargs
        }
        
        if query:
            # Truncate long queries for logging
            db_data["query_preview"] = query[:100] + "..." if len(query) > 100 else query
        
        if duration > 0:
            db_data["rows_per_second"] = round(rows_affected / duration, 2)
        
        self.info(f"🗄️ Database {operation}: {table}", **db_data)
    
    def log_data_quality(self, table: str, total_records: int, **quality_metrics):
        """Log data quality metrics."""
        null_records = quality_metrics.get("null_records", 0)
        duplicate_records = quality_metrics.get("duplicate_records", 0)
        
        # Calculate quality score
        valid_records = total_records - null_records - duplicate_records
        quality_score = (valid_records / total_records * 100) if total_records > 0 else 0
        
        quality_data = {
            "table_name": table,
            "total_records": total_records,
            "valid_records": valid_records,
            "quality_score": round(quality_score, 2),
            **quality_metrics
        }
        
        self.info(f"Data Quality: {table}", **quality_data)
    
    def log_operation_start(self, operation: str, **kwargs):
        """Log the start of an operation."""
        self.info(f"Starting: {operation}", operation_status="start", **kwargs)
    
    def log_operation_end(self, operation: str, success: bool = True, **kwargs):
        """Log the end of an operation."""
        status = "Completed" if success else "Failed"
        self.info(f"{status}: {operation}", operation_status="end", success=success, **kwargs)
    
    def log_pipeline_summary(self):
        """Log comprehensive pipeline execution summary."""
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.metrics["start_time"])
        total_duration = (end_time - start_time).total_seconds()
        
        summary = {
            "pipeline_type": self.pipeline_type,
            "total_duration_seconds": round(total_duration, 2),
            "total_records_processed": self.metrics["total_records_processed"],
            "total_operations": len(self.metrics["operations"]),
            "total_errors": self.metrics["total_errors"],
            "total_warnings": self.metrics["total_warnings"],
            "end_time": end_time.isoformat()
        }
        
        if total_duration > 0:
            summary["overall_records_per_second"] = round(
                self.metrics["total_records_processed"] / total_duration, 2
            )
        
        # Add operation details
        if self.metrics["operations"]:
            total_op_duration = sum(op["duration_seconds"] for op in self.metrics["operations"])
            summary["operations_duration"] = round(total_op_duration, 2)
            summary["fastest_operation"] = min(self.metrics["operations"], key=lambda x: x["duration_seconds"])
            summary["slowest_operation"] = max(self.metrics["operations"], key=lambda x: x["duration_seconds"])
        
        self.info("🎉 Pipeline Execution Summary", **summary)
    
    def get_logger(self) -> logging.Logger:
        """Get the underlying Python logger instance."""
        return self.logger
    
    # Context manager support
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic summary logging."""
        if exc_type:
            self.error(f"Pipeline failed with {exc_type.__name__}: {exc_val}")
        
        self.log_pipeline_summary()


# Context manager for operation tracking
class LoggedOperation:
    """
    Context manager for automatic operation logging with timing.
    
    Usage:
        with LoggedOperation(logger, "data_extraction") as op:
            # Your operation code here
            pass
    """
    
    def __init__(self, logger: ELTPipelineLogger, operation_name: str, **kwargs):
        self.logger = logger
        self.operation_name = operation_name
        self.kwargs = kwargs
        self.start_time = None
        self.records_processed = 0
    
    def update_records(self, count: int):
        """Update the count of records processed during this operation."""
        self.records_processed += count
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.log_operation_start(self.operation_name, **self.kwargs)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        success = exc_type is None
        
        # Log operation completion
        self.logger.log_operation_end(
            self.operation_name,
            success=success,
            duration_seconds=round(duration, 2),
            records_processed=self.records_processed,
            **self.kwargs
        )
        
        # Log performance if records were processed
        if self.records_processed > 0:
            self.logger.log_performance(
                self.operation_name,
                duration,
                self.records_processed
            )
        
        # Log error if operation failed
        if not success:
            self.logger.error(f"Operation '{self.operation_name}' failed: {exc_val}")


# Factory functions for easy logger creation
def get_logger(name: str = None, pipeline_type: str = None, **kwargs) -> ELTPipelineLogger:
    """
    Get a logger instance for ELT pipeline operations.
    
    Args:
        name: Logger name (auto-detected from caller if not provided)
        pipeline_type: Pipeline type (auto-detected if not provided)
        **kwargs: Additional logger configuration
    
    Returns:
        ELTPipelineLogger instance
    """
    if name is None:
        # Auto-detect caller's module name
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'unknown')
    
    # Use caching to avoid creating duplicate loggers
    cache_key = f"{name}_{pipeline_type or 'auto'}"
    
    if cache_key not in ELTPipelineLogger._logger_cache:
        ELTPipelineLogger._logger_cache[cache_key] = ELTPipelineLogger(
            name=name,
            pipeline_type=pipeline_type,
            **kwargs
        )
    
    return ELTPipelineLogger._logger_cache[cache_key]


# Specialized factory functions for different pipeline types
def get_batch_logger(name: str = None, **kwargs) -> ELTPipelineLogger:
    """Get logger specifically for batch pipeline operations."""
    if name is None:
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'unknown')
    return get_logger(name, "batch", **kwargs)


def get_streaming_logger(name: str = None, **kwargs) -> ELTPipelineLogger:
    """Get logger specifically for streaming pipeline operations."""
    if name is None:
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'unknown')
    return get_logger(name, "streaming", **kwargs)


def get_utils_logger(name: str = None, **kwargs) -> ELTPipelineLogger:
    """Get logger specifically for utility operations."""
    if name is None:
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'unknown')
    return get_logger(name, "utils", **kwargs)


def get_pipeline_logger(name: str = None, **kwargs) -> ELTPipelineLogger:
    """Get logger specifically for main pipeline operations."""
    if name is None:
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'unknown')
    return get_logger(name, "pipeline", **kwargs)


# Decorator for automatic function logging
def logged_function(operation_name: str = None, log_args: bool = False, log_result: bool = False):
    """
    Decorator to automatically log function execution.
    
    Args:
        operation_name: Custom operation name (defaults to function name)
        log_args: Whether to log function arguments
        log_result: Whether to log function result
    
    Usage:
        @logged_function("data_extraction", log_args=True)
        def extract_data(table_name: str):
            # Your function code
            return data
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get logger from calling module
            frame = inspect.currentframe().f_back
            module_name = frame.f_globals.get('__name__', 'unknown')
            logger = get_logger(module_name)
            
            op_name = operation_name or func.__name__
            
            # Prepare logging context
            log_context = {"function": func.__name__}
            if log_args:
                log_context["args"] = str(args)[:100]  # Truncate long args
                log_context["kwargs"] = {k: str(v)[:100] for k, v in kwargs.items()}
            
            with LoggedOperation(logger, op_name, **log_context) as op:
                try:
                    result = func(*args, **kwargs)
                    
                    if log_result:
                        result_str = str(result)[:100] if result else "None"
                        logger.debug(f"Function result: {result_str}")
                    
                    return result
                
                except Exception as e:
                    logger.error(f"Function '{func.__name__}' failed: {str(e)}")
                    raise
        
        return wrapper
    return decorator


# Global configuration function
def configure_logging(
    environment: str = None,
    log_level: str = None,
    structured_logging: bool = None,
    enable_file_logging: bool = True,
    enable_console_logging: bool = True
):
    """
    Configure global logging settings for all loggers.
    
    Args:
        environment: Environment name (development, staging, production)
        log_level: Global log level
        structured_logging: Enable structured logging globally
        enable_file_logging: Enable file logging globally
        enable_console_logging: Enable console logging globally
    """
    if environment:
        os.environ["ENVIRONMENT"] = environment
    
    if log_level:
        os.environ["LOG_LEVEL"] = log_level
    
    # Clear logger cache to apply new settings
    ELTPipelineLogger._logger_cache.clear()
    
    # Log configuration change
    logger = get_logger("elt_pipeline.logger_utils.logger_config")
    logger.info("Global logging configuration updated",
               environment=environment or os.getenv("ENVIRONMENT", "development"),
               log_level=log_level or os.getenv("LOG_LEVEL", "INFO"),
               structured_logging=structured_logging,
               file_logging=enable_file_logging,
               console_logging=enable_console_logging)


if __name__ == "__main__":
    # Demo usage
    print("ELT Pipeline Logger Configuration Demo")
    print("=" * 50)
    
    # Test different logger types
    batch_logger = get_batch_logger("demo.batch")
    streaming_logger = get_streaming_logger("demo.streaming")
    utils_logger = get_utils_logger("demo.utils")
    
    # Test basic logging
    batch_logger.info("Batch operation started", records=1000)
    streaming_logger.log_performance("data_generation", 2.5, 500)
    utils_logger.log_database_operation("INSERT", "users", 100, 1.2)
    
    # Test operation context manager
    with LoggedOperation(batch_logger, "mysql_extraction") as op:
        time.sleep(0.1)  # Simulate work
        op.update_records(250)
    
    print("Demo completed! Check logs/ directory for output files.")