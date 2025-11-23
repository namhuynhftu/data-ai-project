"""Kafka topic configurations."""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass(frozen=True)
class TopicConfig:
    """Topic configuration."""
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int = 604800000  # 7 days
    schema_file: Optional[str] = None
    key_field: str = "id"
    
    def get_dlq_name(self) -> str:
        return f"{self.name}.dlq"


class TopicRegistry:
    """Registry of all topic configurations."""
    
    def __init__(self, prefix: Optional[str] = None):
        self.prefix = prefix or os.getenv('TOPIC_PREFIX', '')
        self.topics: Dict[str, TopicConfig] = {}
        self._init_topics()
    
    def _apply_prefix(self, name: str) -> str:
        return f"{self.prefix}.{name}" if self.prefix else name
    
    def _init_topics(self):
        """Initialize all topics."""
        default_partitions = int(os.getenv('KAFKA_DEFAULT_PARTITIONS'))
        default_replication = int(os.getenv('KAFKA_REPLICATION_FACTOR'))
        
        # Users topic
        self._register(TopicConfig(
            name=self._apply_prefix('users'),
            partitions=int(os.getenv('USERS_TOPIC_PARTITIONS', str(default_partitions))),
            replication_factor=default_replication,
            retention_ms=int(os.getenv('USERS_RETENTION_MS', '2592000000')),  # 30 days
            schema_file='users.avsc',
            key_field='user_id'
        ))
        
        # Transactions topic
        self._register(TopicConfig(
            name=self._apply_prefix('transactions'),
            partitions=int(os.getenv('TRANSACTIONS_TOPIC_PARTITIONS', str(default_partitions * 2))),
            replication_factor=default_replication,
            retention_ms=int(os.getenv('TRANSACTIONS_RETENTION_MS', '604800000')),  # 7 days
            schema_file='transactions.avsc',
            key_field='transaction_id'
        ))
        
        # Detailed transactions topic
        self._register(TopicConfig(
            name=self._apply_prefix('detailed_transactions'),
            partitions=int(os.getenv('DETAILED_TRANSACTIONS_TOPIC_PARTITIONS', str(default_partitions * 3))),
            replication_factor=default_replication,
            retention_ms=int(os.getenv('DETAILED_TRANSACTIONS_RETENTION_MS', '1209600000')),  # 14 days
            schema_file='detailed_transactions.avsc',
            key_field='transaction_id'
        ))
    
    def _register(self, topic_config: TopicConfig):
        """Register a topic."""
        if topic_config.partitions < 1:
            raise ValueError(f"Invalid partitions for {topic_config.name}")
        if topic_config.replication_factor < 1:
            raise ValueError(f"Invalid replication for {topic_config.name}")
        
        self.topics[topic_config.name] = topic_config
    
    def get(self, entity: str) -> TopicConfig:
        """Get topic config by entity name."""
        topic_name = self._apply_prefix(entity)
        if topic_name not in self.topics:
            raise ValueError(f"Topic '{entity}' not found")
        return self.topics[topic_name]
    
    def get_schema_file(self, entity: str) -> Optional[str]:
        """Get schema file for entity."""
        return self.get(entity).schema_file
    
    def get_key_field(self, entity: str) -> str:
        """Get key field for entity."""
        return self.get(entity).key_field


# Global registry
_registry: Optional[TopicRegistry] = None


def get_registry() -> TopicRegistry:
    """Get global topic registry."""
    global _registry
    if _registry is None:
        _registry = TopicRegistry()
    return _registry


def get_topic_config(entity: str) -> TopicConfig:
    """Get topic config for entity."""
    return get_registry().get(entity)


def get_schema_file(entity: str) -> Optional[str]:
    """Get schema file for entity."""
    return get_registry().get_schema_file(entity)


def get_key_field(entity: str) -> str:
    """Get key field for entity."""
    return get_registry().get_key_field(entity)


def extract_key(entity: str, record: Dict[str, Any]) -> Optional[str]:
    """Extract key from record."""
    key_field = get_key_field(entity)
    value = record.get(key_field)
    return str(value) if value is not None else None