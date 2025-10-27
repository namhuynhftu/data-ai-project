"""
Streaming Pipeline Logger Utilities

Specialized logging utilities for streaming pipeline operations including:
- Real-time data ingestion
- Event processing
- Stream processing metrics
- PostgreSQL real-time loading

This module provides pre-configured loggers optimized for streaming
operations with appropriate throughput metrics and latency tracking.

Usage:
    from elt_pipeline.logger_utils.streaming_logger import StreamingLogger
    
    logger = StreamingLogger("event_processor")
    logger.log_event_processed(event_type="order", processing_time_ms=15.2)
"""

import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import deque, defaultdict
from .logger_config import get_streaming_logger, LoggedOperation


class StreamingLogger:
    """
    Specialized logger for streaming pipeline operations.
    
    Provides streaming-specific logging methods for real-time data processing,
    throughput monitoring, and latency tracking.
    """
    
    def __init__(self, stream_name: str = None, **kwargs):
        """
        Initialize streaming logger.
        
        Args:
            stream_name: Name of the stream or processor
            **kwargs: Additional logger configuration
        """
        self.stream_name = stream_name
        self.logger = get_streaming_logger(**kwargs)
        
        # Streaming-specific metrics
        self.stream_metrics = {
            "events_processed": 0,
            "events_failed": 0,
            "total_processing_time_ms": 0,
            "throughput_events_per_second": 0,
            "average_latency_ms": 0,
            "last_event_timestamp": None
        }
        
        # Rolling windows for real-time metrics
        self.event_times = deque(maxlen=1000)  # Last 1000 events for throughput
        self.latency_samples = deque(maxlen=100)  # Last 100 latencies for avg
        self.error_counts = defaultdict(int)  # Error type counting
        
        # Performance tracking
        self.start_time = time.time()
        self.last_metrics_report = time.time()
        self.metrics_report_interval = 60  # Report metrics every 60 seconds
    
    # Delegate standard logging methods
    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self.logger.error(message, **kwargs)
        self.stream_metrics["events_failed"] += 1
        
        # Track error types
        error_type = kwargs.get("error_type", "unknown")
        self.error_counts[error_type] += 1
    
    def critical(self, message: str, **kwargs):
        self.logger.critical(message, **kwargs)
        self.stream_metrics["events_failed"] += 1
    
    # Streaming-specific logging methods
    def log_stream_start(self, stream_config: Dict[str, Any] = None):
        """Log the start of stream processing."""
        self.info(f"Starting stream processing: {self.stream_name}",
                 stream_name=self.stream_name,
                 stream_config=stream_config or {},
                 operation_type="stream_start",
                 start_time=datetime.now().isoformat())
    
    def log_event_received(self, event_type: str, event_id: str = None, 
                          source: str = None, **kwargs):
        """Log when an event is received for processing."""
        self.debug(f"Event received: {event_type}",
                  event_type=event_type,
                  event_id=event_id,
                  source=source,
                  operation_type="event_received",
                  timestamp=datetime.now().isoformat(),
                  **kwargs)
    
    def log_event_processed(self, event_type: str, processing_time_ms: float,
                           event_id: str = None, success: bool = True, **kwargs):
        """Log completion of event processing."""
        current_time = time.time()
        
        # Update metrics
        if success:
            self.stream_metrics["events_processed"] += 1
            self.stream_metrics["total_processing_time_ms"] += processing_time_ms
            self.stream_metrics["last_event_timestamp"] = current_time
            
            # Update rolling windows
            self.event_times.append(current_time)
            self.latency_samples.append(processing_time_ms)
            
            # Calculate real-time metrics
            self._update_throughput_metrics()
            self._update_latency_metrics()
            
            self.debug(f"Event processed: {event_type}",
                      event_type=event_type,
                      event_id=event_id,
                      processing_time_ms=round(processing_time_ms, 2),
                      operation_type="event_processed",
                      success=success,
                      **kwargs)
        else:
            self.stream_metrics["events_failed"] += 1
            self.error(f"Event processing failed: {event_type}",
                      event_type=event_type,
                      event_id=event_id,
                      processing_time_ms=round(processing_time_ms, 2),
                      operation_type="event_failed",
                      **kwargs)
        
        # Report metrics periodically
        self._report_metrics_if_needed()
    
    def log_batch_processed(self, batch_size: int, processing_time_ms: float,
                           batch_id: str = None, **kwargs):
        """Log completion of batch processing in streaming context."""
        throughput = batch_size / (processing_time_ms / 1000) if processing_time_ms > 0 else 0
        
        self.info(f"📦 Batch processed: {batch_size} events",
                 batch_size=batch_size,
                 batch_id=batch_id,
                 processing_time_ms=round(processing_time_ms, 2),
                 throughput_events_per_second=round(throughput, 2),
                 operation_type="batch_processed",
                 **kwargs)
        
        # Update overall metrics
        self.stream_metrics["events_processed"] += batch_size
        self.stream_metrics["total_processing_time_ms"] += processing_time_ms
    
    def log_throughput_metrics(self, events_per_second: float, window_size_seconds: int = 60):
        """Log current throughput metrics."""
        self.info(f"Throughput: {events_per_second:.2f} events/sec",
                 events_per_second=round(events_per_second, 2),
                 window_size_seconds=window_size_seconds,
                 operation_type="throughput_metrics")
    
    def log_latency_metrics(self, avg_latency_ms: float, p95_latency_ms: float = None,
                           p99_latency_ms: float = None):
        """Log current latency metrics."""
        latency_data = {
            "avg_latency_ms": round(avg_latency_ms, 2),
            "operation_type": "latency_metrics"
        }
        
        if p95_latency_ms is not None:
            latency_data["p95_latency_ms"] = round(p95_latency_ms, 2)
        if p99_latency_ms is not None:
            latency_data["p99_latency_ms"] = round(p99_latency_ms, 2)
        
        self.info(f"Latency: {avg_latency_ms:.2f}ms avg", **latency_data)
    
    def log_connection_status(self, service: str, status: str, details: str = None):
        """Log connection status to external services."""
        if status.lower() in ["connected", "healthy", "active"]:
            self.info(f"Connection {status}: {service}",
                     service=service,
                     status=status,
                     details=details,
                     operation_type="connection_status")
        else:
            self.warning(f"Connection {status}: {service}",
                        service=service,
                        status=status,
                        details=details,
                        operation_type="connection_status")
    
    def log_backlog_status(self, queue_name: str, backlog_size: int, 
                          max_capacity: int = None):
        """Log queue/buffer backlog status."""
        backlog_percentage = None
        if max_capacity:
            backlog_percentage = (backlog_size / max_capacity) * 100
        
        level = "info"
        if backlog_percentage and backlog_percentage > 80:
            level = "warning"
        elif backlog_percentage and backlog_percentage > 95:
            level = "error"
        
        message = f"Queue status: {queue_name} ({backlog_size} items)"
        log_data = {
            "queue_name": queue_name,
            "backlog_size": backlog_size,
            "max_capacity": max_capacity,
            "backlog_percentage": round(backlog_percentage, 2) if backlog_percentage else None,
            "operation_type": "backlog_status"
        }
        
        getattr(self, level)(message, **log_data)
    
    def log_stream_health_check(self):
        """Log comprehensive stream health metrics."""
        current_time = time.time()
        uptime_seconds = current_time - self.start_time
        
        health_data = {
            "stream_name": self.stream_name,
            "uptime_seconds": round(uptime_seconds, 2),
            "uptime_hours": round(uptime_seconds / 3600, 2),
            "events_processed": self.stream_metrics["events_processed"],
            "events_failed": self.stream_metrics["events_failed"],
            "error_rate": self._calculate_error_rate(),
            "current_throughput": self.stream_metrics["throughput_events_per_second"],
            "average_latency_ms": self.stream_metrics["average_latency_ms"],
            "operation_type": "health_check"
        }
        
        # Add error breakdown if there are errors
        if self.stream_metrics["events_failed"] > 0:
            health_data["error_breakdown"] = dict(self.error_counts)
        
        self.info("Stream Health Check", **health_data)
        return health_data
    
    def _update_throughput_metrics(self):
        """Update throughput based on recent events."""
        current_time = time.time()
        
        # Calculate events in last 60 seconds
        cutoff_time = current_time - 60
        recent_events = [t for t in self.event_times if t >= cutoff_time]
        
        self.stream_metrics["throughput_events_per_second"] = len(recent_events) / 60
    
    def _update_latency_metrics(self):
        """Update latency metrics based on recent samples."""
        if self.latency_samples:
            self.stream_metrics["average_latency_ms"] = sum(self.latency_samples) / len(self.latency_samples)
    
    def _calculate_error_rate(self) -> float:
        """Calculate current error rate percentage."""
        total_events = self.stream_metrics["events_processed"] + self.stream_metrics["events_failed"]
        if total_events == 0:
            return 0.0
        return round((self.stream_metrics["events_failed"] / total_events) * 100, 2)
    
    def _report_metrics_if_needed(self):
        """Report metrics if enough time has passed."""
        current_time = time.time()
        if current_time - self.last_metrics_report >= self.metrics_report_interval:
            self.log_stream_health_check()
            self.last_metrics_report = current_time
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current streaming metrics."""
        return self.stream_metrics.copy()
    
    def get_logger(self):
        """Get the underlying logger instance."""
        return self.logger


# Context manager for streaming operations
class StreamingOperation(LoggedOperation):
    """Extended LoggedOperation for streaming-specific operations."""
    
    def __init__(self, logger: StreamingLogger, operation_name: str,
                 event_type: str = None, event_id: str = None, **kwargs):
        super().__init__(logger.logger, operation_name, **kwargs)
        self.streaming_logger = logger
        self.event_type = event_type
        self.event_id = event_id
        self.operation_type = operation_name
    
    def __enter__(self):
        super().__enter__()
        
        # Log streaming-specific start information
        if self.event_type:
            self.streaming_logger.log_event_received(
                self.event_type, self.event_id
            )
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        success = exc_type is None
        
        # Log streaming-specific completion information
        if self.event_type:
            self.streaming_logger.log_event_processed(
                self.event_type, duration_ms, self.event_id, success
            )
        
        # Call parent's exit method
        super().__exit__(exc_type, exc_val, exc_tb)


# Convenience functions
def get_event_processor_logger(**kwargs) -> StreamingLogger:
    """Get logger specialized for event processing."""
    return StreamingLogger("event_processor", **kwargs)


def get_data_ingestion_logger(**kwargs) -> StreamingLogger:
    """Get logger specialized for data ingestion."""
    return StreamingLogger("data_ingestion", **kwargs)


def get_stream_processor_logger(**kwargs) -> StreamingLogger:
    """Get logger specialized for stream processing."""
    return StreamingLogger("stream_processor", **kwargs)


def get_realtime_loader_logger(**kwargs) -> StreamingLogger:
    """Get logger specialized for real-time data loading."""
    return StreamingLogger("realtime_loader", **kwargs)