"""Kafka client wrapper with Schema Registry integration."""

import time
import logging
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from elt_pipeline.streaming.common.config import config

logger = logging.getLogger(__name__)


@dataclass
class ProducerMetrics:
    messages_produced: int = 0
    messages_failed: int = 0
    
    def update(self, success: bool):
        if success:
            self.messages_produced += 1
        else:
            self.messages_failed += 1


@dataclass
class ConsumerMetrics:
    messages_consumed: int = 0
    messages_failed: int = 0


class KafkaProducerClient:
    """Kafka producer with Avro serialization."""
    
    def __init__(self, topic: str, schema_file: str, config_override: Optional[Dict] = None):
        self.topic = topic
        self.metrics = ProducerMetrics()
        
        # Load config and schema
        producer_config = config.get_producer_config().to_dict()
        if config_override:
            producer_config.update(config_override)
        
        schema_str = config.load_avro_schema(schema_file)
        
        # Initialize Schema Registry and serializers
        schema_registry_config = {'url': config.get_schema_registry_url()}
        self.schema_registry = SchemaRegistryClient(schema_registry_config)
        self.key_serializer = StringSerializer('utf-8')
        self.value_serializer = AvroSerializer(
            self.schema_registry,
            schema_str,
            lambda obj, ctx: obj if isinstance(obj, dict) else obj.__dict__
        )
        
        # Create producer
        self.producer = Producer(producer_config)
        logger.info(f"Producer initialized for topic '{topic}' with schema '{schema_file}'")
    
    def produce(self, value: Dict[str, Any], key: Optional[str] = None,
                headers: Optional[Dict[str, str]] = None) -> None:
        """Produce message to Kafka."""
        try:
            serialized_key = self.key_serializer(key) if key else None
            serialized_value = self.value_serializer(
                value, SerializationContext(self.topic, MessageField.VALUE)
            )
            kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()] if headers else None
            
            self.producer.produce(
                topic=self.topic,
                key=serialized_key,
                value=serialized_value,
                headers=kafka_headers,
                on_delivery=self._delivery_callback
            )
            self.producer.poll(0)
            
        except BufferError:
            logger.warning(f"Producer queue full, flushing...")
            self.producer.flush(10)
            self.produce(value, key, headers)  # Retry
        except Exception as e:
            self.metrics.update(False)
            logger.error(f"Failed to produce message: {e}")
            raise
    
    def _delivery_callback(self, err, msg):
        """Handle delivery reports."""
        if err:
            self.metrics.update(False)
            logger.error(f"Delivery failed: {err}")
        else:
            self.metrics.update(True)
            logger.debug(f"Message delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}")
    
    def flush(self, timeout: float = 30.0) -> int:
        """Flush pending messages."""
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages not flushed")
        return remaining
    
    def close(self):
        """Close producer."""
        logger.info(f"Closing producer. Sent: {self.metrics.messages_produced}, "
                   f"Failed: {self.metrics.messages_failed}")
        self.flush()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class KafkaConsumerClient:
    """Kafka consumer with Avro deserialization."""
    
    def __init__(self, topic: str, schema_file: str, group_id: Optional[str] = None,
                 config_override: Optional[Dict] = None):
        self.topic = topic
        self.metrics = ConsumerMetrics()
        
        # Load config and schema
        consumer_config = config.get_consumer_config(group_id).to_dict()
        if config_override:
            consumer_config.update(config_override)
        
        schema_str = config.load_avro_schema(schema_file)
        
        # Initialize Schema Registry and deserializer
        schema_registry_config = {'url': config.get_schema_registry_url()}
        self.schema_registry = SchemaRegistryClient(schema_registry_config)
        self.value_deserializer = AvroDeserializer(
            self.schema_registry,
            schema_str,
            lambda obj, ctx: obj
        )
        
        # Create consumer
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([topic])
        logger.info(f"Consumer initialized for topic '{topic}' (group: {consumer_config['group.id']})")
    
    def consume_batch(self, batch_size: int = 100, timeout: float = 1.0) -> List[Dict[str, Any]]:
        """Consume batch of messages."""
        messages = []
        
        for _ in range(batch_size):
            msg = self.consumer.poll(timeout)
            
            if msg is None:
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            
            try:
                value = self.value_deserializer(
                    msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
                )
                messages.append(value)
                self.metrics.messages_consumed += 1
            except Exception as e:
                self.metrics.messages_failed += 1
                logger.error(f"Failed to deserialize message at offset {msg.offset()}: {e}")
                continue
        
        return messages
    
    def commit(self, asynchronous: bool = False):
        """Commit offsets."""
        try:
            self.consumer.commit(asynchronous=asynchronous)
        except KafkaException as e:
            logger.error(f"Failed to commit: {e}")
            raise
    
    def close(self):
        """Close consumer."""
        logger.info(f"Closing consumer. Consumed: {self.metrics.messages_consumed}, "
                   f"Failed: {self.metrics.messages_failed}")
        self.consumer.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()