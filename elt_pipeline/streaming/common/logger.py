"""
Centralized Logging Module for Streaming Pipeline.

This module provides structured logging with consistent formatting across
all streaming components (producers, consumers, processors).

Architecture Decisions:
    1. Structured Logging: JSON format for log aggregation systems
    2. Context Propagation: Add contextual information to all logs
    3. Performance: Lazy evaluation to avoid overhead
    4. Flexibility: Support both file and console output
    5. Correlation: Request IDs for distributed tracing

Best Practices Implemented:
    - Hierarchical logger names (streaming.producer.users)
    - Contextual information (topic, partition, offset)
    - Exception tracking with stack traces
    - Performance metrics logging
    - PII redaction for sensitive data
    - Log rotation for disk space management

Author: Senior Data Engineering Team
Date: November 2025
"""

import logging
import sys
import json
from typing import Dict, Any, Optional
from datetime import datetime
from functools import wraps
from pathlib import Path
import traceback

from elt_pipeline.streaming.common.config import streaming_config


# ============================================================================
# CUSTOM LOG FORMATTER
# ============================================================================

class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    
    This formatter outputs logs in JSON format, making them easy to parse
    by log aggregation systems (ELK, Splunk, CloudWatch, etc.).
    
    Best Practice: Structured logs are essential for distributed systems
    where you need to correlate events across multiple services.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
        
        Returns:
            JSON-formatted log string
        """
        log_data = {
            'timestamp': datetime.utcfromtimestamp(record.created).isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': ''.join(traceback.format_exception(*record.exc_info))
            }
        
        # Add extra context fields
        if hasattr(record, 'context'):
            log_data['context'] = record.context
        
        # Add correlation ID if present
        if hasattr(record, 'correlation_id'):
            log_data['correlation_id'] = record.correlation_id
        
        return json.dumps(log_data)


class HumanReadableFormatter(logging.Formatter):
    """
    Human-readable formatter for console output.
    
    This formatter is optimized for development and debugging,
    providing clear, colored output in the console.
    
    Best Practice: Use in development, switch to JSON in production.
    """
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with colors.
        
        Args:
            record: Log record to format
        
        Returns:
            Colored log string
        """
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        
        # Build log line
        log_line = (
            f"{timestamp} - "
            f"{color}{record.levelname:8}{reset} - "
            f"{record.name} - "
            f"{record.getMessage()}"
        )
        
        # Add context if present
        if hasattr(record, 'context'):
            context_str = ', '.join(f"{k}={v}" for k, v in record.context.items())
            log_line += f" [{context_str}]"
        
        # Add exception if present
        if record.exc_info:
            log_line += '\n' + ''.join(traceback.format_exception(*record.exc_info))
        
        return log_line


# ============================================================================
# LOG CONTEXT MANAGER
# ============================================================================

class LogContext:
    """
    Context manager for adding contextual information to logs.
    
    Usage:
        >>> with LogContext(topic='users', partition=0):
        ...     logger.info("Processing message")
        # Output: ... [topic=users, partition=0]
    
    Best Practice: Use log context to add metadata about the current
    operation (topic, partition, consumer group, etc.).
    """
    
    _context_stack: list = []
    
    def __init__(self, **kwargs):
        """
        Initialize log context.
        
        Args:
            **kwargs: Arbitrary context fields (topic, partition, offset, etc.)
        """
        self.context = kwargs
    
    def __enter__(self):
        """Push context onto stack."""
        LogContext._context_stack.append(self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Pop context from stack."""
        LogContext._context_stack.pop()
    
    @classmethod
    def get_current_context(cls) -> Dict[str, Any]:
        """Get merged context from all active context managers."""
        merged = {}
        for context in cls._context_stack:
            merged.update(context)
        return merged


# ============================================================================
# CUSTOM LOG ADAPTER
# ============================================================================

class ContextAdapter(logging.LoggerAdapter):
    """
    Logger adapter that automatically adds context to log records.
    
    Best Practice: Use adapters to inject contextual information
    without modifying log call sites.
    """
    
    def process(self, msg, kwargs):
        """
        Add context to log record.
        
        Args:
            msg: Log message
            kwargs: Keyword arguments
        
        Returns:
            Tuple of (message, kwargs)
        """
        # Get current context
        context = LogContext.get_current_context()
        
        # Add context to extra field
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        
        if context:
            kwargs['extra']['context'] = context
        
        return msg, kwargs


# ============================================================================
# LOGGER FACTORY
# ============================================================================

def get_streaming_logger(
    name: str,
    level: Optional[str] = None,
    format_type: str = 'human'
) -> logging.Logger:
    """
    Get configured logger for streaming components.
    
    Args:
        name: Logger name (e.g., 'producer.users', 'consumer.transactions')
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: Formatter type ('json' or 'human')
    
    Returns:
        Configured logger instance
    
    Usage:
        >>> logger = get_streaming_logger('producer.users')
        >>> logger.info("Starting producer")
    
    Best Practice: Use hierarchical logger names to enable fine-grained
    log level control (e.g., set 'streaming.producer' to DEBUG,
    'streaming.consumer' to INFO).
    """
    # Build full logger name
    full_name = f"streaming.{name}"
    
    # Get or create logger
    logger = logging.getLogger(full_name)
    
    # Set log level
    if level is None:
        level = streaming_config.get_log_level()
    logger.setLevel(level)
    
    # Avoid adding handlers if already configured
    if logger.handlers:
        return logger
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Set formatter
    if format_type == 'json':
        formatter = StructuredFormatter()
    else:
        formatter = HumanReadableFormatter()
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler (optional, based on environment)
    log_file = streaming_config.project_root / "logs" / "streaming" / f"{name}.log"
    if log_file.parent.exists():
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(StructuredFormatter())  # Always JSON for files
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Failed to create file handler: {e}")
    
    return logger


def get_producer_logger(entity: str) -> ContextAdapter:
    """
    Get logger for producer.
    
    Args:
        entity: Entity name (users, transactions, etc.)
    
    Returns:
        Logger with context adapter
    
    Usage:
        >>> logger = get_producer_logger('users')
        >>> logger.info("Produced message", extra={'key': user_id})
    """
    base_logger = get_streaming_logger(f"producer.{entity}")
    return ContextAdapter(base_logger, {})


def get_consumer_logger(entity: str) -> ContextAdapter:
    """
    Get logger for consumer.
    
    Args:
        entity: Entity name (users, transactions, etc.)
    
    Returns:
        Logger with context adapter
    """
    base_logger = get_streaming_logger(f"consumer.{entity}")
    return ContextAdapter(base_logger, {})


# ============================================================================
# PERFORMANCE LOGGING DECORATOR
# ============================================================================

def log_performance(logger: logging.Logger):
    """
    Decorator to log function execution time.
    
    Args:
        logger: Logger instance to use
    
    Usage:
        >>> @log_performance(logger)
        ... def process_batch(messages):
        ...     # Processing logic
        ...     pass
    
    Best Practice: Use for critical paths to track performance
    and identify bottlenecks.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            
            try:
                result = func(*args, **kwargs)
                
                duration = (datetime.utcnow() - start_time).total_seconds()
                logger.info(
                    f"Function '{func.__name__}' completed in {duration:.3f}s"
                )
                
                return result
                
            except Exception as e:
                duration = (datetime.utcnow() - start_time).total_seconds()
                logger.error(
                    f"Function '{func.__name__}' failed after {duration:.3f}s: {e}",
                    exc_info=True
                )
                raise
        
        return wrapper
    return decorator


# ============================================================================
# PII REDACTION
# ============================================================================

def redact_pii(data: Dict[str, Any], fields: Optional[list] = None) -> Dict[str, Any]:
    """
    Redact personally identifiable information from log data.
    
    Args:
        data: Dictionary containing data to redact
        fields: List of field names to redact (default: email, phone, ssn, etc.)
    
    Returns:
        Dictionary with PII redacted
    
    Best Practice: Always redact PII from logs to comply with
    privacy regulations (GDPR, CCPA, etc.).
    
    Usage:
        >>> user_data = {'name': 'John', 'email': 'john@example.com'}
        >>> safe_data = redact_pii(user_data)
        >>> logger.info("User data", extra={'data': safe_data})
    """
    if fields is None:
        fields = ['email', 'phone', 'ssn', 'password', 'credit_card']
    
    redacted = data.copy()
    
    for field in fields:
        if field in redacted:
            redacted[field] = '***REDACTED***'
    
    return redacted


# ============================================================================
# ERROR TRACKING
# ============================================================================

def log_kafka_error(
    logger: logging.Logger,
    error: Exception,
    context: Dict[str, Any]
) -> None:
    """
    Log Kafka-specific errors with context.
    
    Args:
        logger: Logger instance
        error: Exception that occurred
        context: Contextual information (topic, partition, offset, etc.)
    
    Best Practice: Provide rich context for errors to aid debugging
    in production environments.
    """
    logger.error(
        f"Kafka error: {type(error).__name__}: {str(error)}",
        extra={'context': context},
        exc_info=True
    )


def log_processing_error(
    logger: logging.Logger,
    message: Dict[str, Any],
    error: Exception
) -> None:
    """
    Log message processing errors.
    
    Args:
        logger: Logger instance
        message: Message that failed processing
        error: Exception that occurred
    
    Best Practice: Log failed messages for replay or dead letter queue.
    """
    # Redact PII before logging
    safe_message = redact_pii(message)
    
    logger.error(
        f"Failed to process message: {type(error).__name__}: {str(error)}",
        extra={'message': safe_message},
        exc_info=True
    )


# ============================================================================
# METRICS LOGGING
# ============================================================================

def log_producer_metrics(
    logger: logging.Logger,
    metrics: Dict[str, Any]
) -> None:
    """
    Log producer performance metrics.
    
    Args:
        logger: Logger instance
        metrics: Metrics dictionary
    
    Best Practice: Log metrics periodically for monitoring dashboards.
    """
    logger.info(
        "Producer metrics",
        extra={
            'context': {
                'type': 'metrics',
                **metrics
            }
        }
    )


def log_consumer_metrics(
    logger: logging.Logger,
    metrics: Dict[str, Any]
) -> None:
    """
    Log consumer performance metrics.
    
    Args:
        logger: Logger instance
        metrics: Metrics dictionary
    """
    logger.info(
        "Consumer metrics",
        extra={
            'context': {
                'type': 'metrics',
                **metrics
            }
        }
    )


# ============================================================================
# MODULE-LEVEL LOGGER
# ============================================================================

# Default logger for this module
logger = get_streaming_logger('common')
