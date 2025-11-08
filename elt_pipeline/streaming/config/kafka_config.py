"""
Kafka-Specific Configuration Module for Streaming Pipeline.

This module provides Kafka topic configurations, serialization settings,
and topic-specific utilities that complement the main StreamingConfig.

Architecture Decisions:
    1. Topic Configuration: Centralized topic settings (partitions, replication, retention)
    2. Schema Mapping: Direct mapping between topics and Avro schemas
    3. Message Key Strategy: Configurable key extraction for partitioning
    4. Dead Letter Queue: Error handling with DLQ topics
    5. Topic Naming: Environment-aware topic naming with prefixes

Best Practices Implemented:
    - PEP 8 compliant (line length, naming conventions)
    - Type hints for all public methods (PEP 484)
    - Comprehensive docstrings (PEP 257)
    - Immutable configurations using frozen dataclasses
    - Environment-specific topic configurations

Author: Senior Data Engineering Team
Date: November 2025
"""

import os
from typing import Dict, Any, Optional, List, Callable, Literal
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS FOR TOPIC CONFIGURATION
# ============================================================================

class TopicType(str, Enum):
    """
    Topic type enumeration for classification.
    
    Best Practice: Categorize topics by purpose to enable
    different retention policies and monitoring strategies.
    """
    TRANSACTIONAL = "transactional"  # Business transactions
    DIMENSIONAL = "dimensional"       # Reference/dimension data
    EVENT = "event"                  # Domain events
    DLQ = "dlq"                      # Dead letter queue
    AUDIT = "audit"                  # Audit logs


class CleanupPolicy(str, Enum):
    """
    Kafka topic cleanup policies.
    
    Reference: https://kafka.apache.org/documentation/#topicconfigs_cleanup.policy
    """
    DELETE = "delete"           # Delete old segments (time/size based)
    COMPACT = "compact"         # Keep only latest value per key
    COMPACT_DELETE = "compact,delete"  # Both compaction and deletion


class CompressionType(str, Enum):
    """
    Kafka compression types.
    
    Performance Notes:
        - none: No compression (lowest CPU, highest bandwidth)
        - gzip: High compression ratio (high CPU, low bandwidth)
        - snappy: Balanced (moderate CPU, moderate bandwidth)
        - lz4: Fast compression (low CPU, good bandwidth)
        - zstd: Best compression ratio (moderate CPU, lowest bandwidth)
    """
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


# ============================================================================
# TOPIC CONFIGURATION DATA CLASSES
# ============================================================================

@dataclass(frozen=True)
class TopicConfig:
    """
    Immutable Kafka topic configuration.
    
    Attributes:
        name: Topic name (environment prefix will be applied)
        partitions: Number of partitions (affects parallelism)
        replication_factor: Number of replicas (affects durability)
        retention_ms: Retention time in milliseconds (-1 = infinite)
        retention_bytes: Retention size in bytes per partition (-1 = infinite)
        cleanup_policy: Cleanup strategy (delete/compact/both)
        compression_type: Compression algorithm
        min_insync_replicas: Minimum in-sync replicas for acks=all
        segment_ms: Time before forcing segment rollover
        max_message_bytes: Maximum message size in bytes
        schema_file: Associated Avro schema filename
        topic_type: Classification for monitoring and policies
        key_field: Field name to use as message key (for partitioning)
        enable_dlq: Whether to create associated DLQ topic
    
    Best Practice: Use frozen dataclass to prevent runtime modification
    of topic configurations.
    """
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int = 604800000  # 7 days default
    retention_bytes: int = -1  # Unlimited
    cleanup_policy: CleanupPolicy = CleanupPolicy.DELETE
    compression_type: CompressionType = CompressionType.SNAPPY
    min_insync_replicas: int = 2
    segment_ms: int = 604800000  # 7 days
    max_message_bytes: int = 1048576  # 1 MB
    schema_file: Optional[str] = None
    topic_type: TopicType = TopicType.EVENT
    key_field: str = "id"  # Default key field
    enable_dlq: bool = True
    
    def to_admin_config(self) -> Dict[str, Any]:
        """
        Convert to Kafka AdminClient topic configuration format.
        
        Returns:
            Dictionary compatible with AdminClient.create_topics()
        
        Usage:
            >>> from confluent_kafka.admin import AdminClient, NewTopic
            >>> topic_config = TopicConfig(name="users", partitions=3, replication_factor=2)
            >>> admin_config = topic_config.to_admin_config()
            >>> new_topic = NewTopic(
            ...     topic=admin_config['name'],
            ...     num_partitions=admin_config['num_partitions'],
            ...     replication_factor=admin_config['replication_factor'],
            ...     config=admin_config['config']
            ... )
        """
        return {
            'name': self.name,
            'num_partitions': self.partitions,
            'replication_factor': self.replication_factor,
            'config': {
                'retention.ms': str(self.retention_ms),
                'retention.bytes': str(self.retention_bytes),
                'cleanup.policy': self.cleanup_policy.value,
                'compression.type': self.compression_type.value,
                'min.insync.replicas': str(self.min_insync_replicas),
                'segment.ms': str(self.segment_ms),
                'max.message.bytes': str(self.max_message_bytes)
            }
        }
    
    def get_dlq_name(self) -> str:
        """
        Get Dead Letter Queue topic name.
        
        Returns:
            DLQ topic name following naming convention
        
        Best Practice: DLQ topics follow pattern: <original_topic>.dlq
        """
        return f"{self.name}.dlq"
    
    def get_retry_name(self, attempt: int = 1) -> str:
        """
        Get retry topic name for specific attempt.
        
        Args:
            attempt: Retry attempt number (1-based)
        
        Returns:
            Retry topic name
        
        Best Practice: Retry topics follow pattern: <original_topic>.retry.<N>
        """
        return f"{self.name}.retry.{attempt}"


@dataclass(frozen=True)
class MessageKeyExtractor:
    """
    Configuration for extracting message keys from records.
    
    The message key determines which partition receives the message,
    ensuring ordering for records with the same key.
    
    Attributes:
        field_name: Field name to use as key
        extractor: Custom function to extract key from record
        default_key: Default key if field is missing
    
    Best Practice: Use meaningful keys for:
        1. Maintaining order (same key = same partition)
        2. Log compaction (latest value per key)
        3. Co-partitioning (join operations)
    """
    field_name: str
    extractor: Optional[Callable[[Dict[str, Any]], str]] = None
    default_key: Optional[str] = None
    
    def extract_key(self, record: Dict[str, Any]) -> Optional[str]:
        """
        Extract key from record using configured strategy.
        
        Args:
            record: Message record dictionary
        
        Returns:
            Extracted key as string or None
        
        Priority:
            1. Custom extractor function (if provided)
            2. Direct field lookup
            3. Default key (if configured)
        """
        # Use custom extractor if provided
        if self.extractor:
            try:
                return self.extractor(record)
            except Exception as e:
                logger.warning(f"Key extractor failed: {e}, falling back to field lookup")
        
        # Try direct field lookup
        key = record.get(self.field_name)
        if key is not None:
            return str(key)
        
        # Fall back to default
        if self.default_key is not None:
            logger.debug(f"Using default key '{self.default_key}' for record")
            return self.default_key
        
        return None


# ============================================================================
# KAFKA TOPIC REGISTRY
# ============================================================================

class KafkaTopicRegistry:
    """
    Central registry for all Kafka topic configurations.
    
    This class provides a single source of truth for topic configurations,
    ensuring consistency across producers and consumers.
    
    Design Pattern: Registry Pattern
        - Centralized topic configuration
        - Lazy initialization
        - Environment-aware topic naming
    
    Best Practices:
        - All topics defined in one place
        - Type-safe configuration access
        - Automatic DLQ topic creation
        - Schema file mapping
    """
    
    def __init__(self, environment_prefix: Optional[str] = None):
        """
        Initialize topic registry with environment prefix.
        
        Args:
            environment_prefix: Prefix for topic names (e.g., 'prod', 'dev')
        
        Best Practice: Use prefixes to isolate environments:
            - dev.users, staging.users, prod.users
        """
        self._environment_prefix = environment_prefix or os.getenv('TOPIC_PREFIX', '')
        self._topics: Dict[str, TopicConfig] = {}
        self._initialize_topics()
    
    def _apply_prefix(self, topic_name: str) -> str:
        """
        Apply environment prefix to topic name.
        
        Args:
            topic_name: Base topic name
        
        Returns:
            Prefixed topic name if prefix is configured
        """
        if self._environment_prefix:
            return f"{self._environment_prefix}.{topic_name}"
        return topic_name
    
    def _initialize_topics(self) -> None:
        """
        Initialize all topic configurations.
        
        Best Practice: Define all topics upfront for:
            1. Explicit configuration management
            2. Automated topic creation
            3. Documentation and discovery
            4. Consistent naming conventions
        
        Topology:
            - users: Dimensional data (compacted for CDC)
            - transactions: Transactional data (delete policy)
            - detailed_transactions: Transactional with rich metadata
            - *.dlq: Dead letter queues for error handling
        """
        # Get environment-specific settings
        default_partitions = int(os.getenv('KAFKA_DEFAULT_PARTITIONS', '3'))
        default_replication = int(os.getenv('KAFKA_REPLICATION_FACTOR', '2'))
        
        # Users Topic - Dimensional Data
        # Use compaction for CDC (Change Data Capture) pattern
        self._register_topic(TopicConfig(
            name=self._apply_prefix('users'),
            partitions=int(os.getenv('USERS_TOPIC_PARTITIONS', str(default_partitions))),
            replication_factor=default_replication,
            retention_ms=int(os.getenv('USERS_RETENTION_MS', '2592000000')),  # 30 days
            cleanup_policy=CleanupPolicy.COMPACT_DELETE,  # Keep latest + time-based deletion
            compression_type=CompressionType.SNAPPY,
            min_insync_replicas=max(1, default_replication - 1),
            schema_file='users.avsc',
            topic_type=TopicType.DIMENSIONAL,
            key_field='user_id',  # Partition by user_id
            enable_dlq=True
        ))
        
        # Transactions Topic - Transactional Data
        # Higher throughput, shorter retention
        self._register_topic(TopicConfig(
            name=self._apply_prefix('transactions'),
            partitions=int(os.getenv('TRANSACTIONS_TOPIC_PARTITIONS', str(default_partitions * 2))),
            replication_factor=default_replication,
            retention_ms=int(os.getenv('TRANSACTIONS_RETENTION_MS', '604800000')),  # 7 days
            cleanup_policy=CleanupPolicy.DELETE,  # Time-based deletion
            compression_type=CompressionType.LZ4,  # Fast compression for high throughput
            min_insync_replicas=max(1, default_replication - 1),
            segment_ms=86400000,  # 1 day segments
            max_message_bytes=2097152,  # 2 MB
            schema_file='transactions.avsc',
            topic_type=TopicType.TRANSACTIONAL,
            key_field='transaction_id',  # Partition by transaction_id
            enable_dlq=True
        ))
        
        # Detailed Transactions Topic - Rich Transactional Data
        # Larger messages, more partitions for parallelism
        self._register_topic(TopicConfig(
            name=self._apply_prefix('detailed_transactions'),
            partitions=int(os.getenv('DETAILED_TRANSACTIONS_TOPIC_PARTITIONS', str(default_partitions * 3))),
            replication_factor=default_replication,
            retention_ms=int(os.getenv('DETAILED_TRANSACTIONS_RETENTION_MS', '1209600000')),  # 14 days
            cleanup_policy=CleanupPolicy.DELETE,
            compression_type=CompressionType.ZSTD,  # Best compression for large messages
            min_insync_replicas=max(1, default_replication - 1),
            segment_ms=86400000,  # 1 day segments
            max_message_bytes=5242880,  # 5 MB (larger for nested objects)
            schema_file='detailed_transactions.avsc',
            topic_type=TopicType.TRANSACTIONAL,
            key_field='transaction_id',  # Partition by transaction_id
            enable_dlq=True
        ))
        
        logger.info(f"Initialized {len(self._topics)} topic configurations")
    
    def _register_topic(self, topic_config: TopicConfig) -> None:
        """
        Register a topic configuration.
        
        Args:
            topic_config: Topic configuration to register
        
        Best Practice: Validate topic configuration on registration
        to fail fast if configuration is invalid.
        """
        # Validate configuration
        if topic_config.partitions < 1:
            raise ValueError(f"Topic {topic_config.name}: partitions must be >= 1")
        if topic_config.replication_factor < 1:
            raise ValueError(f"Topic {topic_config.name}: replication_factor must be >= 1")
        if topic_config.min_insync_replicas > topic_config.replication_factor:
            raise ValueError(
                f"Topic {topic_config.name}: min_insync_replicas "
                f"({topic_config.min_insync_replicas}) cannot exceed "
                f"replication_factor ({topic_config.replication_factor})"
            )
        
        # Register main topic
        self._topics[topic_config.name] = topic_config
        
        # Register DLQ topic if enabled
        if topic_config.enable_dlq:
            dlq_config = TopicConfig(
                name=topic_config.get_dlq_name(),
                partitions=topic_config.partitions,
                replication_factor=topic_config.replication_factor,
                retention_ms=topic_config.retention_ms * 2,  # Longer retention for DLQ
                cleanup_policy=CleanupPolicy.DELETE,
                compression_type=topic_config.compression_type,
                min_insync_replicas=1,  # Lower requirement for DLQ
                schema_file=None,  # DLQ stores error metadata
                topic_type=TopicType.DLQ,
                key_field='original_key',
                enable_dlq=False  # No DLQ for DLQ
            )
            self._topics[dlq_config.name] = dlq_config
    
    def get_topic_config(self, entity: str) -> TopicConfig:
        """
        Get topic configuration by entity name.
        
        Args:
            entity: Entity name (users, transactions, detailed_transactions)
        
        Returns:
            Topic configuration
        
        Raises:
            ValueError: If topic not found
        """
        topic_name = self._apply_prefix(entity)
        if topic_name not in self._topics:
            available = [t.replace(self._environment_prefix + '.', '') 
                        for t in self._topics.keys() 
                        if not t.endswith('.dlq')]
            raise ValueError(
                f"Topic configuration not found for '{entity}'. "
                f"Available topics: {available}"
            )
        return self._topics[topic_name]
    
    def get_dlq_config(self, entity: str) -> Optional[TopicConfig]:
        """
        Get DLQ topic configuration for entity.
        
        Args:
            entity: Entity name
        
        Returns:
            DLQ topic configuration or None if DLQ not enabled
        """
        main_config = self.get_topic_config(entity)
        if not main_config.enable_dlq:
            return None
        
        dlq_name = main_config.get_dlq_name()
        return self._topics.get(dlq_name)
    
    def get_all_topics(self, include_dlq: bool = True) -> List[TopicConfig]:
        """
        Get all registered topic configurations.
        
        Args:
            include_dlq: Whether to include DLQ topics
        
        Returns:
            List of topic configurations
        """
        if include_dlq:
            return list(self._topics.values())
        
        return [
            config for config in self._topics.values()
            if config.topic_type != TopicType.DLQ
        ]
    
    def get_topics_by_type(self, topic_type: TopicType) -> List[TopicConfig]:
        """
        Get all topics of specific type.
        
        Args:
            topic_type: Topic type to filter by
        
        Returns:
            List of matching topic configurations
        """
        return [
            config for config in self._topics.values()
            if config.topic_type == topic_type
        ]
    
    def get_schema_file(self, entity: str) -> Optional[str]:
        """
        Get Avro schema filename for entity.
        
        Args:
            entity: Entity name
        
        Returns:
            Schema filename or None if not configured
        """
        config = self.get_topic_config(entity)
        return config.schema_file
    
    def get_key_extractor(self, entity: str) -> MessageKeyExtractor:
        """
        Get message key extractor for entity.
        
        Args:
            entity: Entity name
        
        Returns:
            MessageKeyExtractor configured for the entity
        
        Best Practice: Use consistent key extraction strategy
        to ensure proper partitioning and ordering.
        """
        config = self.get_topic_config(entity)
        return MessageKeyExtractor(
            field_name=config.key_field,
            default_key=None  # No default - enforce key presence
        )


# ============================================================================
# SERIALIZATION CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class SerializerConfig:
    """
    Configuration for Avro serialization.
    
    Attributes:
        auto_register_schemas: Auto-register schemas with Schema Registry
        normalize_schemas: Normalize schema before registration
        use_latest_version: Always use latest schema version
        subject_name_strategy: Subject naming strategy for Schema Registry
    
    Subject Naming Strategies:
        - topic_name: <topic>-key or <topic>-value (default)
        - record_name: <namespace>.<name>
        - topic_record_name: <topic>-<namespace>.<name>
    
    Best Practice: Use topic_name strategy for simple use cases,
    record_name for schema reuse across topics.
    """
    auto_register_schemas: bool = True
    normalize_schemas: bool = True
    use_latest_version: bool = True
    subject_name_strategy: Literal['topic_name', 'record_name', 'topic_record_name'] = 'topic_name'
    
    def to_serializer_config(self) -> Dict[str, Any]:
        """
        Convert to AvroSerializer configuration format.
        
        Returns:
            Dictionary compatible with AvroSerializer
        """
        return {
            'auto.register.schemas': self.auto_register_schemas,
            'normalize.schemas': self.normalize_schemas,
            'use.latest.version': self.use_latest_version,
            'subject.name.strategy': self.subject_name_strategy
        }


# ============================================================================
# GLOBAL REGISTRY INSTANCE
# ============================================================================

# Create global registry instance
# Will be initialized with environment prefix from TOPIC_PREFIX env var
_topic_registry: Optional[KafkaTopicRegistry] = None


def get_topic_registry() -> KafkaTopicRegistry:
    """
    Get or create global topic registry instance.
    
    Returns:
        Global KafkaTopicRegistry instance
    
    Best Practice: Use singleton pattern for registry to ensure
    consistent configuration across the application.
    """
    global _topic_registry
    if _topic_registry is None:
        _topic_registry = KafkaTopicRegistry()
    return _topic_registry


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_topic_config(entity: str) -> TopicConfig:
    """
    Convenience function to get topic configuration.
    
    Args:
        entity: Entity name (users, transactions, detailed_transactions)
    
    Returns:
        Topic configuration
    """
    return get_topic_registry().get_topic_config(entity)


def get_topic_name(entity: str) -> str:
    """
    Convenience function to get full topic name.
    
    Args:
        entity: Entity name
    
    Returns:
        Full topic name with environment prefix
    """
    return get_topic_registry().get_topic_config(entity).name


def get_schema_file(entity: str) -> Optional[str]:
    """
    Convenience function to get schema filename.
    
    Args:
        entity: Entity name
    
    Returns:
        Avro schema filename
    """
    return get_topic_registry().get_schema_file(entity)


def get_key_extractor(entity: str) -> MessageKeyExtractor:
    """
    Convenience function to get key extractor.
    
    Args:
        entity: Entity name
    
    Returns:
        MessageKeyExtractor for the entity
    """
    return get_topic_registry().get_key_extractor(entity)


def get_all_topic_configs(include_dlq: bool = True) -> List[TopicConfig]:
    """
    Convenience function to get all topic configurations.
    
    Args:
        include_dlq: Whether to include DLQ topics
    
    Returns:
        List of all topic configurations
    """
    return get_topic_registry().get_all_topics(include_dlq)


def get_serializer_config() -> SerializerConfig:
    """
    Get default serializer configuration.
    
    Returns:
        SerializerConfig with sensible defaults
    
    Environment Variables:
        - AUTO_REGISTER_SCHEMAS: Set to 'false' to disable auto-registration
        - SUBJECT_NAME_STRATEGY: Override default subject naming strategy
    """
    return SerializerConfig(
        auto_register_schemas=os.getenv('AUTO_REGISTER_SCHEMAS', 'true').lower() == 'true',
        normalize_schemas=True,
        use_latest_version=os.getenv('USE_LATEST_VERSION', 'true').lower() == 'true',
        subject_name_strategy=os.getenv('SUBJECT_NAME_STRATEGY', 'topic_name')
    )


# ============================================================================
# TOPIC VALIDATION
# ============================================================================

def validate_topic_configs() -> Dict[str, Any]:
    """
    Validate all topic configurations.
    
    Returns:
        Validation report with errors and warnings
    
    Best Practice: Call this on application startup to ensure
    all topic configurations are valid.
    """
    errors = []
    warnings = []
    
    try:
        registry = get_topic_registry()
        topics = registry.get_all_topics(include_dlq=False)
        
        if not topics:
            errors.append("No topics configured")
        
        for topic in topics:
            # Validate schema file exists
            if topic.schema_file:
                from pathlib import Path
                schema_path = Path(__file__).parent / "schemas" / topic.schema_file
                if not schema_path.exists():
                    warnings.append(
                        f"Schema file not found for topic '{topic.name}': {topic.schema_file}"
                    )
            
            # Check partition count
            if topic.partitions > 100:
                warnings.append(
                    f"Topic '{topic.name}' has high partition count: {topic.partitions}"
                )
            
            # Check retention
            if topic.retention_ms > 2592000000:  # 30 days
                warnings.append(
                    f"Topic '{topic.name}' has long retention: "
                    f"{topic.retention_ms / 86400000:.1f} days"
                )
        
    except Exception as e:
        errors.append(f"Topic registry initialization failed: {e}")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings,
        'topic_count': len(topics) if topics else 0
    }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == '__main__':
    """
    Example usage and testing of KafkaTopicRegistry.
    
    Run this file directly to test topic configurations:
        python -m elt_pipeline.streaming.config.kafka_config
    """
    import json
    
    print("=" * 80)
    print("Kafka Topic Configuration")
    print("=" * 80)
    
    # Get registry
    registry = get_topic_registry()
    
    # Display all topics
    print(f"\nConfigured Topics:")
    print("-" * 80)
    
    for topic in registry.get_all_topics(include_dlq=False):
        print(f"\nTopic: {topic.name}")
        print(f"  Type: {topic.topic_type.value}")
        print(f"  Partitions: {topic.partitions}")
        print(f"  Replication: {topic.replication_factor}")
        print(f"  Retention: {topic.retention_ms / 86400000:.1f} days")
        print(f"  Cleanup: {topic.cleanup_policy.value}")
        print(f"  Compression: {topic.compression_type.value}")
        print(f"  Schema: {topic.schema_file or 'None'}")
        print(f"  Key Field: {topic.key_field}")
        print(f"  DLQ Enabled: {topic.enable_dlq}")
        
        if topic.enable_dlq:
            dlq_config = registry.get_dlq_config(topic.name.split('.')[-1])
            if dlq_config:
                print(f"  DLQ Topic: {dlq_config.name}")
    
    # Validate configurations
    print("\n" + "=" * 80)
    print("Validation Report")
    print("=" * 80)
    
    validation = validate_topic_configs()
    print(f"\nValid: {validation['valid']}")
    print(f"Topic Count: {validation['topic_count']}")
    
    if validation['errors']:
        print(f"\nErrors:")
        for error in validation['errors']:
            print(f"  ❌ {error}")
    
    if validation['warnings']:
        print(f"\nWarnings:")
        for warning in validation['warnings']:
            print(f"  ⚠️  {warning}")
    
    if not validation['errors'] and not validation['warnings']:
        print("\n✅ All topic configurations are valid!")
    
    # Display serializer config
    print("\n" + "=" * 80)
    print("Serializer Configuration")
    print("=" * 80)
    
    serializer_config = get_serializer_config()
    print(f"\nAuto Register Schemas: {serializer_config.auto_register_schemas}")
    print(f"Normalize Schemas: {serializer_config.normalize_schemas}")
    print(f"Use Latest Version: {serializer_config.use_latest_version}")
    print(f"Subject Name Strategy: {serializer_config.subject_name_strategy}")
