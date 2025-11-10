"""Structured logging for streaming pipeline."""

import logging
import sys
import json
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from elt_pipeline.streaming.common.config import config


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.utcfromtimestamp(record.created).isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName
        }
        
        if record.exc_info:
            log_data['exception'] = str(record.exc_info[1])
        
        if hasattr(record, 'context'):
            log_data['context'] = record.context
        
        return json.dumps(log_data)


class ColorFormatter(logging.Formatter):
    """Color formatter for console output."""
    
    COLORS = {
        'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARNING': '\033[33m',
        'ERROR': '\033[31m', 'CRITICAL': '\033[35m', 'RESET': '\033[0m'
    }
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        
        return (f"{timestamp} - {color}{record.levelname:8}{reset} - "
                f"{record.name} - {record.getMessage()}")


def get_logger(name: str, level: Optional[str] = None, use_json: bool = False) -> logging.Logger:
    """
    Get configured logger.
    
    Args:
        name: Logger name (e.g., 'producer.users')
        level: Log level (defaults to INFO)
        use_json: Use JSON formatter (default: colored console)
    """
    full_name = f"streaming.{name}"
    logger = logging.getLogger(full_name)
    
    if logger.handlers:
        return logger
    
    import os
    log_level = level or os.getenv('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JSONFormatter() if use_json else ColorFormatter())
    logger.addHandler(console_handler)
    
    # Optional file handler
    log_dir = config.streaming_root / "logs"
    if log_dir.exists():
        log_file = log_dir / f"{name.replace('.', '_')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
    
    return logger


def get_producer_logger(entity: str) -> logging.Logger:
    """Get logger for producer."""
    return get_logger(f"producer.{entity}")


def get_consumer_logger(entity: str) -> logging.Logger:
    """Get logger for consumer."""
    return get_logger(f"consumer.{entity}")