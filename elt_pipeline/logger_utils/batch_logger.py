"""
Batch Pipeline Logger Utilities

Specialized logging utilities for batch pipeline operations including:
- MySQL data extraction
- MinIO data storage
- Snowflake data loading
- dbt transformations

This module provides pre-configured loggers optimized for batch processing
with appropriate performance metrics and data quality tracking.

Usage:
    from elt_pipeline.logger_utils.batch_logger import BatchLogger
    
    logger = BatchLogger("mysql_extraction")
    logger.log_extraction_performance(table="customers", records=10000, duration=5.2)
"""

import time
from typing import Dict, Any, Optional
from .logger_config import get_batch_logger, LoggedOperation


class BatchLogger:
    """
    Specialized logger for batch pipeline operations.
    
    Provides batch-specific logging methods for common operations like
    data extraction, transformation, and loading.
    """
    
    def __init__(self, operation_name: str = None, **kwargs):
        """
        Initialize batch logger.
        
        Args:
            operation_name: Name of the batch operation
            **kwargs: Additional logger configuration
        """
        self.operation_name = operation_name
        self.logger = get_batch_logger(**kwargs)
        
        # Batch-specific metrics
        self.batch_metrics = {
            "tables_processed": 0,
            "total_records_extracted": 0,
            "total_records_loaded": 0,
            "failed_operations": 0,
            "data_quality_issues": 0
        }
    
    # Delegate standard logging methods
    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self.logger.error(message, **kwargs)
        self.batch_metrics["failed_operations"] += 1
    
    def critical(self, message: str, **kwargs):
        self.logger.critical(message, **kwargs)
        self.batch_metrics["failed_operations"] += 1
    
    # Batch-specific logging methods
    def log_extraction_start(self, source: str, table: str, **kwargs):
        """Log the start of data extraction."""
        self.info(f"📤 Starting extraction: {table} from {source}",
                 source=source,
                 table=table,
                 operation_type="extraction_start",
                 **kwargs)
    
    def log_extraction_complete(self, source: str, table: str, records_extracted: int, 
                               duration: float, **kwargs):
        """Log completion of data extraction."""
        self.batch_metrics["tables_processed"] += 1
        self.batch_metrics["total_records_extracted"] += records_extracted
        
        self.logger.log_performance(
            f"extraction_{table}",
            duration,
            records_extracted,
            source=source,
            table=table
        )
        
        self.info(f"✅ Extraction complete: {table}",
                 source=source,
                 table=table,
                 records_extracted=records_extracted,
                 duration_seconds=round(duration, 2),
                 operation_type="extraction_complete",
                 **kwargs)
    
    def log_transformation_start(self, transformation_type: str, input_table: str, **kwargs):
        """Log the start of data transformation."""
        self.info(f"🔄 Starting transformation: {transformation_type}",
                 transformation_type=transformation_type,
                 input_table=input_table,
                 operation_type="transformation_start",
                 **kwargs)
    
    def log_transformation_complete(self, transformation_type: str, input_table: str,
                                   output_table: str, records_processed: int,
                                   duration: float, **kwargs):
        """Log completion of data transformation."""
        self.logger.log_performance(
            f"transformation_{transformation_type}",
            duration,
            records_processed,
            input_table=input_table,
            output_table=output_table
        )
        
        self.info(f"✅ Transformation complete: {transformation_type}",
                 transformation_type=transformation_type,
                 input_table=input_table,
                 output_table=output_table,
                 records_processed=records_processed,
                 duration_seconds=round(duration, 2),
                 operation_type="transformation_complete",
                 **kwargs)
    
    def log_loading_start(self, destination: str, table: str, **kwargs):
        """Log the start of data loading."""
        self.info(f"📥 Starting loading: {table} to {destination}",
                 destination=destination,
                 table=table,
                 operation_type="loading_start",
                 **kwargs)
    
    def log_loading_complete(self, destination: str, table: str, records_loaded: int,
                            duration: float, **kwargs):
        """Log completion of data loading."""
        self.batch_metrics["total_records_loaded"] += records_loaded
        
        self.logger.log_performance(
            f"loading_{table}",
            duration,
            records_loaded,
            destination=destination,
            table=table
        )
        
        self.info(f"✅ Loading complete: {table}",
                 destination=destination,
                 table=table,
                 records_loaded=records_loaded,
                 duration_seconds=round(duration, 2),
                 operation_type="loading_complete",
                 **kwargs)
    
    def log_data_quality_check(self, table: str, quality_results: Dict[str, Any]):
        """Log data quality check results."""
        quality_score = quality_results.get("quality_score", 0)
        
        # Create a copy without quality_score to avoid duplication
        other_metrics = {k: v for k, v in quality_results.items() if k != "quality_score"}
        
        if quality_score < 95:  # Threshold for quality issues
            self.batch_metrics["data_quality_issues"] += 1
            self.warning(f"⚠️ Data quality issue detected: {table}",
                        table=table,
                        quality_score=quality_score,
                        **other_metrics)
        else:
            self.info(f"✅ Data quality check passed: {table}",
                     table=table,
                     quality_score=quality_score,
                     **other_metrics)
    
    def log_batch_summary(self):
        """Log comprehensive batch processing summary."""
        summary = {
            "operation_name": self.operation_name,
            "tables_processed": self.batch_metrics["tables_processed"],
            "total_records_extracted": self.batch_metrics["total_records_extracted"],
            "total_records_loaded": self.batch_metrics["total_records_loaded"],
            "failed_operations": self.batch_metrics["failed_operations"],
            "data_quality_issues": self.batch_metrics["data_quality_issues"],
            "success_rate": self._calculate_success_rate()
        }
        
        self.info("🎉 Batch Processing Summary", **summary)
        return summary
    
    def _calculate_success_rate(self) -> float:
        """Calculate overall success rate."""
        total_operations = (self.batch_metrics["tables_processed"] + 
                           self.batch_metrics["failed_operations"])
        
        if total_operations == 0:
            return 100.0
        
        successful_operations = self.batch_metrics["tables_processed"]
        return round((successful_operations / total_operations) * 100, 2)
    
    def get_logger(self):
        """Get the underlying logger instance."""
        return self.logger


# Context manager for batch operations
class BatchOperation(LoggedOperation):
    """Extended LoggedOperation for batch-specific operations."""
    
    def __init__(self, logger: BatchLogger, operation_name: str, 
                 source: str = None, destination: str = None, table: str = None, **kwargs):
        super().__init__(logger.logger, operation_name, **kwargs)
        self.batch_logger = logger
        self.source = source
        self.destination = destination
        self.table = table
        self.operation_type = operation_name
    
    def __enter__(self):
        super().__enter__()
        
        # Log batch-specific start information
        if self.operation_type == "extraction" and self.source and self.table:
            self.batch_logger.log_extraction_start(self.source, self.table)
        elif self.operation_type == "loading" and self.destination and self.table:
            self.batch_logger.log_loading_start(self.destination, self.table)
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        success = exc_type is None
        
        # Log batch-specific completion information
        if success:
            if self.operation_type == "extraction" and self.source and self.table:
                self.batch_logger.log_extraction_complete(
                    self.source, self.table, self.records_processed, duration
                )
            elif self.operation_type == "loading" and self.destination and self.table:
                self.batch_logger.log_loading_complete(
                    self.destination, self.table, self.records_processed, duration
                )
        
        # Call parent's exit method
        super().__exit__(exc_type, exc_val, exc_tb)


# Convenience functions
def get_mysql_logger(**kwargs) -> BatchLogger:
    """Get logger specialized for MySQL operations."""
    return BatchLogger("mysql_operations", **kwargs)


def get_minio_logger(**kwargs) -> BatchLogger:
    """Get logger specialized for MinIO operations."""
    return BatchLogger("minio_operations", **kwargs)


def get_snowflake_logger(**kwargs) -> BatchLogger:
    """Get logger specialized for Snowflake operations."""
    return BatchLogger("snowflake_operations", **kwargs)


def get_dbt_logger(**kwargs) -> BatchLogger:
    """Get logger specialized for dbt operations."""
    return BatchLogger("dbt_operations", **kwargs)