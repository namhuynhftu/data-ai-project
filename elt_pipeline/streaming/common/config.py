"""
Configuration Management Module for Streaming Pipeline.

This module provides a centralized, type-safe configuration management system
following the Single Responsibility Principle and Dependency Injection pattern.

Architecture Decisions:
    1. Singleton Pattern: Ensures single configuration instance across the application
    2. Environment-First: Environment variables override default values (12-Factor App)
    3. Lazy Loading: Schemas loaded only when needed (performance optimization)
    4. Type Safety: All methods use type hints for better IDE support and validation
    5. Fail-Fast: Invalid configurations raise exceptions immediately

Best Practices Implemented:
    - PEP 8 compliant (line length, naming conventions)
    - Type hints for all public methods (PEP 484)
    - Comprehensive docstrings (PEP 257)
    - Immutable configuration values where possible
    - Clear separation between different config domains (Kafka, DB, Schemas)

Author: Senior Data Engineering Team
Date: November 2025
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Literal
from dataclasses import dataclass, field
from functools import lru_cache
from enum import Enum
from datetime import datetime
import logging

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS FOR ENVIRONMENT & VALIDATION
# ============================================================================

class Environment(str, Enum):
    """
    Application environment enumeration.
    
    Best Practice: Use enums to enforce valid environment values
    and enable environment-specific configurations.
    """
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


class ValidationError(Exception):
    """Custom exception for configuration validation errors."""
    pass


# ============================================================================
# DATA CLASSES FOR TYPE SAFETY
# ============================================================================

@dataclass(frozen=True)
class KafkaProducerConfig:
    """
    Immutable Kafka producer configuration.
    
    Attributes:
        bootstrap_servers: Comma-separated list of Kafka brokers
        acks: Acknowledgment level ('all', '1', '0')
        retries: Number of retries for failed sends
        enable_idempotence: Enable idempotent producer (exactly-once)
        compression_type: Compression algorithm ('none', 'gzip', 'snappy', 'lz4', 'zstd')
        linger_ms: Time to wait before sending batch
        batch_size: Maximum batch size in bytes
        max_in_flight: Max unacknowledged requests per connection
    
    Best Practice: Use frozen dataclass to prevent accidental mutation
    """
    bootstrap_servers: str
    acks: str = 'all'
    retries: int = 2147483647  # Max int32 value
    enable_idempotence: bool = True
    compression_type: str = 'snappy'
    linger_ms: int = 10
    batch_size: int = 16384
    max_in_flight: int = 5
    client_id: str = 'streaming-producer'
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to confluent-kafka compatible dictionary."""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': self.acks,
            'retries': self.retries,
            'enable.idempotence': self.enable_idempotence,
            'compression.type': self.compression_type,
            'linger.ms': self.linger_ms,
            'batch.size': self.batch_size,
            'max.in.flight.requests.per.connection': self.max_in_flight,
            'client.id': self.client_id
        }


@dataclass(frozen=True)
class KafkaConsumerConfig:
    """
    Immutable Kafka consumer configuration.
    
    Attributes:
        bootstrap_servers: Comma-separated list of Kafka brokers
        group_id: Consumer group identifier
        auto_offset_reset: Offset reset policy ('earliest', 'latest', 'none')
        enable_auto_commit: Auto-commit offsets (False for manual control)
        isolation_level: Transaction isolation ('read_uncommitted', 'read_committed')
        max_poll_interval_ms: Max time between polls before rebalance
        session_timeout_ms: Session timeout for consumer heartbeat
    
    Best Practice: Disable auto-commit for exactly-once semantics
    """
    bootstrap_servers: str
    group_id: str = 'streaming-consumers'
    auto_offset_reset: str = 'earliest'
    enable_auto_commit: bool = False
    isolation_level: str = 'read_committed'
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 10000
    client_id: str = 'streaming-consumer'
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to confluent-kafka compatible dictionary."""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'isolation.level': self.isolation_level,
            'max.poll.interval.ms': self.max_poll_interval_ms,
            'session.timeout.ms': self.session_timeout_ms,
            'client.id': self.client_id
        }


@dataclass(frozen=True)
class PostgresConfig:
    """
    Immutable PostgreSQL connection configuration.
    
    Best Practice: Use frozen dataclass to prevent credential mutation
    
    Security Enhancement: Supports both direct password and secrets manager lookup
    """
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = 'streaming'
    
    def __post_init__(self):
        """
        Validate configuration after initialization.
        
        Raises:
            ValidationError: If any field is invalid
        
        Best Practice: Fail-fast validation prevents runtime errors
        """
        if not self.host:
            raise ValidationError("PostgreSQL host cannot be empty")
        if not (1 <= self.port <= 65535):
            raise ValidationError(f"Invalid PostgreSQL port: {self.port}")
        if not self.database:
            raise ValidationError("PostgreSQL database name cannot be empty")
        if not self.user:
            raise ValidationError("PostgreSQL user cannot be empty")
        if not self.password:
            raise ValidationError("PostgreSQL password cannot be empty")
    
    def get_connection_string(self, hide_password: bool = False) -> str:
        """
        Generate SQLAlchemy connection string.
        
        Args:
            hide_password: If True, replace password with '***' for logging
        
        Returns:
            PostgreSQL connection URI
        
        Security Note: Use hide_password=True when logging connection strings
        """
        password = '***' if hide_password else self.password
        return (
            f"postgresql+psycopg2://{self.user}:{password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


# ============================================================================
# MAIN CONFIGURATION CLASS
# ============================================================================

class StreamingConfig:
    """
    Centralized configuration manager for streaming pipeline.
    
    This class follows the Singleton pattern and provides type-safe access
    to all configuration parameters. It prioritizes environment variables
    over hardcoded defaults, following the 12-Factor App methodology.
    
    Design Patterns:
        - Singleton: Single instance across application
        - Lazy Loading: Schemas loaded on-demand
        - Strategy: Different config strategies per environment
        - Factory: Creates typed config objects
    
    Usage:
        >>> config = StreamingConfig()
        >>> producer_config = config.get_producer_config()
        >>> schema = config.load_avro_schema('users.avsc')
    """
    
    _instance: Optional['StreamingConfig'] = None
    _validated: bool = False
    
    def __new__(cls) -> 'StreamingConfig':
        """
        Implement Singleton pattern.
        
        Best Practice: Ensure only one configuration instance exists
        to prevent inconsistent state across the application.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """
        Initialize configuration paths and cached values.
        
        Best Practice: Validate paths on initialization to fail fast
        if configuration is incomplete.
        """
        # Calculate project paths
        self.project_root: Path = Path(__file__).parent.parent.parent.parent
        self.streaming_root: Path = Path(__file__).parent.parent
        self.config_dir: Path = self.streaming_root / "config"
        self.schema_dir: Path = self.config_dir / "schemas"
        
        # Get current environment
        self._environment = self._get_environment()
        
        # Validate critical paths exist
        if not self.config_dir.exists():
            raise FileNotFoundError(f"Config directory not found: {self.config_dir}")
        if not self.schema_dir.exists():
            logger.warning(f"Schema directory not found: {self.schema_dir}")
    
    @staticmethod
    def _get_environment() -> Environment:
        """
        Get current application environment.
        
        Returns:
            Environment enum value
        
        Raises:
            ValidationError: If environment is invalid
        
        Best Practice: Explicitly set environment to prevent
        accidental production deployments with dev config.
        """
        env_str = os.getenv('ENVIRONMENT', 'development').lower()
        try:
            return Environment(env_str)
        except ValueError:
            valid_envs = [e.value for e in Environment]
            raise ValidationError(
                f"Invalid environment '{env_str}'. "
                f"Must be one of: {valid_envs}"
            )
    
    @property
    def environment(self) -> Environment:
        """Get current environment."""
        return self._environment
    
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self._environment == Environment.PRODUCTION
    
    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self._environment == Environment.DEVELOPMENT
    
    # ========================================================================
    # KAFKA CONFIGURATION
    # ========================================================================
    
    @staticmethod
    def get_kafka_bootstrap_servers() -> str:
        """
        Get Kafka broker addresses.
        
        Environment Variable: KAFKA_BOOTSTRAP_SERVERS
        Default: kafka_broker:9092
        
        Returns:
            Comma-separated list of broker addresses
        
        Best Practice: Use service discovery in production (Consul, Eureka)
        """
        return os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka_broker:9092')
    
    @staticmethod
    def get_schema_registry_url() -> str:
        """
        Get Schema Registry endpoint URL.
        
        Environment Variable: SCHEMA_REGISTRY_URL
        Default: http://schema_registry:8081
        
        Returns:
            Schema Registry HTTP endpoint
        """
        return os.getenv('SCHEMA_REGISTRY_URL', 'http://schema_registry:8081')
    
    def get_producer_config(
        self,
        client_id: Optional[str] = None
    ) -> KafkaProducerConfig:
        """
        Get type-safe Kafka producer configuration.
        
        Args:
            client_id: Optional custom client identifier
        
        Returns:
            Immutable producer configuration object
        
        Best Practice: Returns immutable dataclass to prevent accidental
        configuration changes during runtime.
        """
        return KafkaProducerConfig(
            bootstrap_servers=self.get_kafka_bootstrap_servers(),
            acks=os.getenv('KAFKA_ACKS', 'all'),
            compression_type=os.getenv('KAFKA_COMPRESSION', 'snappy'),
            linger_ms=int(os.getenv('KAFKA_LINGER_MS', '10')),
            batch_size=int(os.getenv('KAFKA_BATCH_SIZE', '16384')),
            client_id=client_id or os.getenv('PRODUCER_CLIENT_ID', 'streaming-producer')
        )
    
    def get_consumer_config(
        self,
        group_id: Optional[str] = None,
        client_id: Optional[str] = None
    ) -> KafkaConsumerConfig:
        """
        Get type-safe Kafka consumer configuration.
        
        Args:
            group_id: Optional custom consumer group ID
            client_id: Optional custom client identifier
        
        Returns:
            Immutable consumer configuration object
        
        Best Practice: Separate consumer groups per use case to enable
        independent processing and replay capabilities.
        """
        return KafkaConsumerConfig(
            bootstrap_servers=self.get_kafka_bootstrap_servers(),
            group_id=group_id or os.getenv('CONSUMER_GROUP_ID', 'streaming-consumers'),
            auto_offset_reset=os.getenv('AUTO_OFFSET_RESET', 'earliest'),
            client_id=client_id or os.getenv('CONSUMER_CLIENT_ID', 'streaming-consumer')
        )
    
    def get_schema_registry_config(self) -> Dict[str, str]:
        """
        Get Schema Registry client configuration.
        
        Returns:
            Schema Registry configuration dictionary
        """
        return {
            'url': self.get_schema_registry_url()
        }
    
    # ========================================================================
    # POSTGRESQL CONFIGURATION
    # ========================================================================
    
    def get_postgres_config(self) -> PostgresConfig:
        """
        Get type-safe PostgreSQL configuration.
        
        Returns:
            Immutable PostgreSQL configuration object
        
        Security Best Practice: Never log or print this object as it
        contains database credentials. Use get_connection_string(hide_password=True)
        for logging.
        
        Raises:
            ValidationError: If required environment variables are missing in production
        """
        # In production, require explicit configuration
        if self.is_production:
            required_vars = [
                'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB',
                'POSTGRES_USER', 'POSTGRES_PASSWORD'
            ]
            missing = [var for var in required_vars if not os.getenv(var)]
            if missing:
                raise ValidationError(
                    f"Missing required PostgreSQL configuration in production: {missing}"
                )
        
        # Get password from environment or secrets manager
        password = self._get_secret('POSTGRES_PASSWORD', 'password')
        
        return PostgresConfig(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5433')),
            database=os.getenv('POSTGRES_DB', 'streaming_db'),
            user=os.getenv('POSTGRES_USER', 'user'),
            password=password,
            schema=os.getenv('POSTGRES_SCHEMA', 'streaming')
        )
    
    @staticmethod
    def _get_secret(env_var: str, default: str = None) -> str:
        """
        Get secret from environment or secrets manager.
        
        Args:
            env_var: Environment variable name
            default: Default value if not found (only used in development)
        
        Returns:
            Secret value
        
        Best Practice: Support multiple secret sources:
        1. Direct environment variable
        2. File path (Docker secrets, Kubernetes secrets)
        3. AWS Secrets Manager / HashiCorp Vault (future)
        
        Usage:
            # Direct: export POSTGRES_PASSWORD=mypassword
            # File: export POSTGRES_PASSWORD_FILE=/run/secrets/db_password
        """
        # Try direct environment variable first
        value = os.getenv(env_var)
        if value:
            return value
        
        # Try file-based secret (Docker/Kubernetes pattern)
        file_var = f"{env_var}_FILE"
        file_path = os.getenv(file_var)
        if file_path and Path(file_path).exists():
            with open(file_path, 'r') as f:
                return f.read().strip()
        
        # Fall back to default (only in non-production)
        if default is not None:
            env = os.getenv('ENVIRONMENT', 'development')
            if env != 'production':
                logger.warning(
                    f"Using default value for {env_var}. "
                    f"Set {env_var} or {file_var} in production."
                )
                return default
        
        raise ValidationError(
            f"Secret not found: {env_var}. "
            f"Set {env_var} or {file_var} environment variable."
        )
    
    # ========================================================================
    # SCHEMA MANAGEMENT
    # ========================================================================
    
    @lru_cache(maxsize=32)
    def load_avro_schema(self, schema_file: str) -> str:
        """
        Load Avro schema file with caching.
        
        Args:
            schema_file: Schema filename (e.g., 'users.avsc')
        
        Returns:
            Avro schema as JSON string
        
        Raises:
            FileNotFoundError: If schema file doesn't exist
        
        Performance Optimization: Uses LRU cache to avoid repeated file I/O.
        Cache size of 32 is sufficient for typical streaming applications.
        
        Best Practice: Cache schemas in memory since they're read frequently
        but rarely change during runtime.
        """
        schema_path = self.schema_dir / schema_file
        if not schema_path.exists():
            raise FileNotFoundError(
                f"Schema file not found: {schema_path}\n"
                f"Available schemas: {list(self.schema_dir.glob('*.avsc'))}"
            )
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_content = f.read()
        
        logger.debug(f"Loaded Avro schema: {schema_file}")
        return schema_content
    
    @lru_cache(maxsize=1)
    def load_metadata(self) -> Dict[str, Any]:
        """
        Load metadata.json configuration with caching.
        
        Returns:
            Complete metadata dictionary
        
        Raises:
            FileNotFoundError: If metadata.json doesn't exist
            json.JSONDecodeError: If JSON is malformed
        
        Best Practice: Cache metadata since it's used frequently by
        producers and consumers but rarely changes.
        """
        metadata_path = self.config_dir / "metadata.json"
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
        
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        logger.debug("Loaded metadata configuration")
        return metadata
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata for specific table.
        
        Args:
            table_name: Name of the table (users, transactions, etc.)
        
        Returns:
            Table metadata dictionary
        
        Raises:
            ValueError: If table not found in metadata
        """
        metadata = self.load_metadata()
        if table_name not in metadata:
            available_tables = list(metadata.keys())
            raise ValueError(
                f"Table '{table_name}' not found in metadata.\n"
                f"Available tables: {available_tables}"
            )
        return metadata[table_name]
    
    # ========================================================================
    # TOPIC CONFIGURATION
    # ========================================================================
    
    @staticmethod
    def get_topic_name(entity: str) -> str:
        """
        Get Kafka topic name for entity with optional prefix.
        
        Args:
            entity: Entity name (users, transactions, etc.)
        
        Returns:
            Full topic name with environment prefix if configured
        
        Best Practice: Use topic prefixes in multi-tenant environments
        to prevent naming collisions (e.g., 'prod.users', 'dev.users').
        """
        topic_prefix = os.getenv('TOPIC_PREFIX', '')
        if topic_prefix:
            return f"{topic_prefix}.{entity}"
        return entity
    
    @staticmethod
    def get_all_topics() -> Dict[str, str]:
        """
        Get all configured topic names.
        
        Returns:
            Dictionary mapping entity names to topic names
        """
        entities = ['users', 'transactions', 'detailed_transactions']
        return {
            entity: StreamingConfig.get_topic_name(entity)
            for entity in entities
        }
    
    # ========================================================================
    # PRODUCER/CONSUMER TUNING
    # ========================================================================
    
    @staticmethod
    def get_producer_rate_limit(producer_name: str) -> float:
        """
        Get rate limit (seconds between messages) for producer.
        
        Args:
            producer_name: Name of the producer (users, transactions, etc.)
        
        Returns:
            Sleep time in seconds between message production
        
        Best Practice: Rate limiting prevents overwhelming downstream
        systems and allows graceful backpressure handling.
        """
        rate_limits = {
            'users': float(os.getenv('USER_PRODUCER_RATE', '0.1')),
            'transactions': float(os.getenv('TRANSACTION_PRODUCER_RATE', '0.05')),
            'detailed_transactions': float(os.getenv('DETAILED_PRODUCER_RATE', '0.02'))
        }
        return rate_limits.get(producer_name, 0.1)
    
    @staticmethod
    def get_consumer_batch_size(consumer_name: str) -> int:
        """
        Get batch size for consumer.
        
        Args:
            consumer_name: Name of the consumer (users, transactions, etc.)
        
        Returns:
            Number of messages to process per batch
        
        Best Practice: Batch processing improves throughput and reduces
        database round trips. Tune based on message size and latency requirements.
        """
        batch_sizes = {
            'users': int(os.getenv('USER_CONSUMER_BATCH', '100')),
            'transactions': int(os.getenv('TRANSACTION_CONSUMER_BATCH', '200')),
            'detailed_transactions': int(os.getenv('DETAILED_CONSUMER_BATCH', '500'))
        }
        return batch_sizes.get(consumer_name, 100)
    
    # ========================================================================
    # LOGGING CONFIGURATION
    # ========================================================================
    
    @staticmethod
    def get_log_level() -> str:
        """
        Get logging level.
        
        Returns:
            Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
        Best Practice: Use INFO in production, DEBUG in development.
        """
        return os.getenv('LOG_LEVEL', 'INFO').upper()
    
    @staticmethod
    def get_log_format() -> str:
        """
        Get structured logging format.
        
        Returns:
            Python logging format string
        
        Best Practice: Include timestamp, logger name, level, and message
        for troubleshooting. Consider JSON format for log aggregation systems.
        """
        return os.getenv(
            'LOG_FORMAT',
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # ========================================================================
    # CONFIGURATION VALIDATION
    # ========================================================================
    
    def validate(self) -> Dict[str, Any]:
        """
        Validate entire configuration and return validation report.
        
        Returns:
            Dictionary containing validation results:
            - valid: bool - Overall validation status
            - errors: List[str] - Validation errors
            - warnings: List[str] - Validation warnings
            - checks: Dict[str, bool] - Individual check results
        
        Best Practice: Call this on application startup to fail fast
        if configuration is incomplete or invalid.
        
        Usage:
            >>> config = StreamingConfig()
            >>> result = config.validate()
            >>> if not result['valid']:
            ...     logger.error(f"Invalid config: {result['errors']}")
            ...     sys.exit(1)
        """
        errors = []
        warnings = []
        checks = {}
        
        # Check environment
        try:
            env = self.environment
            checks['environment'] = True
        except Exception as e:
            errors.append(f"Invalid environment: {e}")
            checks['environment'] = False
        
        # Check Kafka connectivity
        try:
            bootstrap = self.get_kafka_bootstrap_servers()
            if not bootstrap:
                errors.append("Kafka bootstrap servers not configured")
                checks['kafka_bootstrap'] = False
            else:
                checks['kafka_bootstrap'] = True
        except Exception as e:
            errors.append(f"Kafka configuration error: {e}")
            checks['kafka_bootstrap'] = False
        
        # Check Schema Registry
        try:
            registry_url = self.get_schema_registry_url()
            if not registry_url:
                errors.append("Schema Registry URL not configured")
                checks['schema_registry'] = False
            else:
                checks['schema_registry'] = True
        except Exception as e:
            errors.append(f"Schema Registry configuration error: {e}")
            checks['schema_registry'] = False
        
        # Check PostgreSQL configuration
        try:
            postgres_config = self.get_postgres_config()
            checks['postgres'] = True
        except ValidationError as e:
            errors.append(f"PostgreSQL configuration error: {e}")
            checks['postgres'] = False
        except Exception as e:
            errors.append(f"Unexpected PostgreSQL error: {e}")
            checks['postgres'] = False
        
        # Check schemas directory
        if not self.schema_dir.exists():
            warnings.append(f"Schema directory not found: {self.schema_dir}")
            checks['schema_dir'] = False
        else:
            # Check for required schema files
            required_schemas = ['users.avsc', 'transactions.avsc', 'detailed_transactions.avsc']
            missing_schemas = []
            for schema in required_schemas:
                if not (self.schema_dir / schema).exists():
                    missing_schemas.append(schema)
            
            if missing_schemas:
                warnings.append(f"Missing schema files: {missing_schemas}")
                checks['schema_dir'] = False
            else:
                checks['schema_dir'] = True
        
        # Check metadata.json
        try:
            self.load_metadata()
            checks['metadata'] = True
        except FileNotFoundError:
            warnings.append("metadata.json not found")
            checks['metadata'] = False
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON in metadata.json: {e}")
            checks['metadata'] = False
        
        # Production-specific checks
        if self.is_production:
            # Ensure no default passwords
            if os.getenv('POSTGRES_PASSWORD') == 'password':
                errors.append("Default password detected in production")
                checks['secure_passwords'] = False
            else:
                checks['secure_passwords'] = True
            
            # Ensure proper log level
            log_level = self.get_log_level()
            if log_level == 'DEBUG':
                warnings.append("DEBUG logging enabled in production (performance impact)")
        
        # Overall validation result
        valid = len(errors) == 0
        
        # Mark as validated
        if valid:
            self._validated = True
        
        return {
            'valid': valid,
            'environment': self.environment.value,
            'errors': errors,
            'warnings': warnings,
            'checks': checks,
            'validated_at': datetime.utcnow().isoformat() + 'Z'
        }
    
    def require_validation(self) -> None:
        """
        Require that configuration has been validated.
        
        Raises:
            ValidationError: If configuration hasn't been validated
        
        Best Practice: Call this in critical paths to ensure
        configuration was validated on startup.
        """
        if not self._validated:
            raise ValidationError(
                "Configuration has not been validated. "
                "Call config.validate() on application startup."
            )
    
    # ========================================================================
    # HEALTH CHECKS
    # ========================================================================
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on external dependencies.
        
        Returns:
            Dictionary containing health status for each dependency
        
        Best Practice: Use for readiness probes in Kubernetes or
        health check endpoints in load balancers.
        
        Usage:
            >>> health = config.health_check()
            >>> if health['overall'] == 'healthy':
            ...     return 200, health
            ... else:
            ...     return 503, health
        """
        from datetime import datetime
        import socket
        
        health = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'environment': self.environment.value,
            'checks': {}
        }
        
        # Check Kafka connectivity
        try:
            bootstrap = self.get_kafka_bootstrap_servers()
            host, port = bootstrap.split(':')[0], int(bootstrap.split(':')[1])
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            
            health['checks']['kafka'] = {
                'status': 'healthy' if result == 0 else 'unhealthy',
                'endpoint': bootstrap
            }
        except Exception as e:
            health['checks']['kafka'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
        
        # Check Schema Registry connectivity
        try:
            import urllib.request
            registry_url = self.get_schema_registry_url()
            
            with urllib.request.urlopen(f"{registry_url}/subjects", timeout=2) as response:
                if response.status == 200:
                    health['checks']['schema_registry'] = {
                        'status': 'healthy',
                        'endpoint': registry_url
                    }
                else:
                    health['checks']['schema_registry'] = {
                        'status': 'unhealthy',
                        'status_code': response.status
                    }
        except Exception as e:
            health['checks']['schema_registry'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
        
        # Check PostgreSQL connectivity
        try:
            postgres_config = self.get_postgres_config()
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((postgres_config.host, postgres_config.port))
            sock.close()
            
            health['checks']['postgres'] = {
                'status': 'healthy' if result == 0 else 'unhealthy',
                'endpoint': f"{postgres_config.host}:{postgres_config.port}"
            }
        except Exception as e:
            health['checks']['postgres'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
        
        # Overall health
        statuses = [check['status'] for check in health['checks'].values()]
        if all(s == 'healthy' for s in statuses):
            health['overall'] = 'healthy'
        elif any(s == 'healthy' for s in statuses):
            health['overall'] = 'degraded'
        else:
            health['overall'] = 'unhealthy'
        
        return health


# ============================================================================
# GLOBAL SINGLETON INSTANCE
# ============================================================================

# Create single global instance (lazy initialization on import)
streaming_config = StreamingConfig()


# ============================================================================
# CONVENIENCE FUNCTIONS (Backward Compatibility)
# ============================================================================

def get_producer_config(client_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function for backward compatibility.
    
    Args:
        client_id: Optional producer client ID
    
    Returns:
        Producer configuration dictionary
    """
    return streaming_config.get_producer_config(client_id).to_dict()


def get_consumer_config(
    group_id: Optional[str] = None,
    client_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function for backward compatibility.
    
    Args:
        group_id: Optional consumer group ID
        client_id: Optional consumer client ID
    
    Returns:
        Consumer configuration dictionary
    """
    return streaming_config.get_consumer_config(group_id, client_id).to_dict()


def get_postgres_config() -> PostgresConfig:
    """Convenience function to get PostgreSQL configuration."""
    return streaming_config.get_postgres_config()


def load_avro_schema(schema_file: str) -> str:
    """Convenience function to load Avro schema."""
    return streaming_config.load_avro_schema(schema_file)


def get_topic_name(entity: str) -> str:
    """Convenience function to get topic name."""
    return streaming_config.get_topic_name(entity)
