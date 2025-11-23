"""Base producer for Kafka streaming."""

import time
import signal
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

from elt_pipeline.streaming.common.kafka_client import KafkaProducerClient
from elt_pipeline.streaming.common.logger import get_producer_logger
from elt_pipeline.streaming.common.config import config
from elt_pipeline.streaming.config.kafka_config import get_topic_config, get_schema_file, extract_key


@dataclass
class ProducerStats:
    """Producer statistics."""
    messages_sent: int = 0
    messages_failed: int = 0
    start_time: Optional[datetime] = None
    error_count: int = 0
    
    def record_success(self):
        self.messages_sent += 1
        self.error_count = 0
    
    def record_failure(self):
        self.messages_failed += 1
        self.error_count += 1
    
    def get_throughput(self) -> float:
        if not self.start_time:
            return 0.0
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        return self.messages_sent / elapsed if elapsed > 0 else 0.0


class BaseProducer(ABC):
    """
    Abstract base producer.
    """
    
    def __init__(self, entity: str, rate_limit: Optional[float] = None, max_errors: int = 10):
        self.entity = entity
        self.rate_limit = rate_limit or config.get_producer_rate_limit(entity)
        self.max_errors = max_errors
        
        # Get topic config
        self.topic_config = get_topic_config(entity)
        self.topic_name = self.topic_config.name
        self.schema_file = get_schema_file(entity)
        
        # State
        self.stats = ProducerStats()
        self.shutdown_requested = False
        self.logger = get_producer_logger(entity)
        self.producer: Optional[KafkaProducerClient] = None
        
        # Handle shutdown signals
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
        self.logger.info(f"Producer initialized for '{entity}' (topic: {self.topic_name})")
    
    def _handle_signal(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.warning(f"Received signal {signum}, shutting down...")
        self.shutdown_requested = True
    
    def setup(self):
        """Initialize Kafka producer."""
        self.logger.info("Setting up Kafka producer...")
        self.producer = KafkaProducerClient(self.topic_name, self.schema_file)
        self.stats.start_time = datetime.utcnow()
        self.logger.info("Producer setup complete")
    
    def teardown(self):
        """Cleanup resources."""
        self.logger.info("Tearing down producer...")
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.producer = None
        self._log_stats()
        self.logger.info("Producer teardown complete")
    
    @abstractmethod
    def generate_record(self) -> Dict[str, Any]:
        """Generate a record (must be implemented by subclass)."""
        pass
    
    def produce_record(self, record: Dict[str, Any]) -> bool:
        """Produce record to Kafka."""
        if not self.producer:
            return False
        
        try:
            key = extract_key(self.entity, record)
            headers = {
                'producer': self.entity,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            
            self.producer.produce(value=record, key=key, headers=headers)
            self.stats.record_success()
            return True
            
        except Exception as e:
            self.stats.record_failure()
            self.logger.error(f"Failed to produce record: {e}", exc_info=True)
            return False
    
    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker should trip."""
        if self.stats.error_count >= self.max_errors:
            self.logger.error(f"Circuit breaker tripped: {self.stats.error_count} errors")
            return False
        return True
    
    def _log_stats(self):
        """Log statistics."""
        self.logger.info(
            f"Stats - Sent: {self.stats.messages_sent}, "
            f"Failed: {self.stats.messages_failed}, "
            f"Throughput: {self.stats.get_throughput():.2f} msg/s"
        )
    
    def run(self, max_messages: Optional[int] = None, stats_interval: int = 1000):
        """Run producer loop."""
        self.logger.info(f"Starting producer (max: {max_messages or 'unlimited'})")
        
        try:
            self.setup()
            message_count = 0
            
            while not self.shutdown_requested:
                if max_messages and message_count >= max_messages:
                    break
                
                if not self._check_circuit_breaker():
                    break
                
                try:
                    record = self.generate_record()
                    if self.produce_record(record):
                        message_count += 1
                        if message_count % stats_interval == 0:
                            self._log_stats()
                    
                    if self.rate_limit:
                        time.sleep(self.rate_limit)
                        
                except Exception as e:
                    self.logger.error(f"Error in produce loop: {e}", exc_info=True)
                    self.stats.record_failure()
                    time.sleep(1)
            
            self.logger.info("Producer stopped")
            
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            raise
        finally:
            self.teardown()