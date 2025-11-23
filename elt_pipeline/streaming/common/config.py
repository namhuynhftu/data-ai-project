"""Configuration management for streaming pipeline."""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
from functools import lru_cache

try:
    from dotenv import load_dotenv
    env_name = os.getenv('ENVIRONMENT', 'development')
    project_root = Path(__file__).parent.parent.parent.parent
    env_file = project_root / 'config' / 'app' / f'{env_name}.env'
    load_dotenv(env_file if env_file.exists() else None)
except ImportError:
    pass


@dataclass(frozen=True)
class KafkaProducerConfig:
    bootstrap_servers: str
    acks: str = 'all'
    enable_idempotence: bool = True
    compression_type: str = 'snappy'
    client_id: str = 'streaming-producer'
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': self.acks,
            'enable.idempotence': self.enable_idempotence,
            'compression.type': self.compression_type,
            'client.id': self.client_id
        }


@dataclass(frozen=True)
class KafkaConsumerConfig:
    bootstrap_servers: str
    group_id: str = 'streaming-consumers'
    auto_offset_reset: str = 'earliest'
    enable_auto_commit: bool = False
    client_id: str = 'streaming-consumer'
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'client.id': self.client_id
        }


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = 'streaming'
    
    def get_connection_string(self, hide_password: bool = False) -> str:
        pwd = '***' if hide_password else self.password
        return f"postgresql+psycopg2://{self.user}:{pwd}@{self.host}:{self.port}/{self.database}"


class StreamingConfig:
    """Centralized config manager using singleton pattern."""
    
    _instance: Optional['StreamingConfig'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        self.project_root = Path(__file__).parent.parent.parent.parent
        self.streaming_root = Path(__file__).parent.parent
        self.config_dir = self.streaming_root / "config"
        self.schema_dir = self.config_dir / "schemas"
        self.is_production = os.getenv('ENVIRONMENT')
    
    # Kafka Configuration
    @staticmethod
    def get_kafka_bootstrap_servers() -> str:
        return os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    
    @staticmethod
    def get_schema_registry_url() -> str:
        return os.getenv('SCHEMA_REGISTRY_URL')
    
    def get_producer_config(self, client_id: Optional[str] = None) -> KafkaProducerConfig:
        return KafkaProducerConfig(
            bootstrap_servers=self.get_kafka_bootstrap_servers(),
            acks=os.getenv('KAFKA_ACKS'),
            compression_type=os.getenv('KAFKA_COMPRESSION'),
            client_id=client_id or os.getenv('PRODUCER_CLIENT_ID')
        )
    
    def get_consumer_config(self, group_id: Optional[str] = None, 
                          client_id: Optional[str] = None) -> KafkaConsumerConfig:
        return KafkaConsumerConfig(
            bootstrap_servers=self.get_kafka_bootstrap_servers(),
            group_id=group_id or os.getenv('CONSUMER_GROUP_ID'),
            auto_offset_reset=os.getenv('AUTO_OFFSET_RESET'),
            client_id=client_id or os.getenv('CONSUMER_CLIENT_ID')
        )
    
    # PostgreSQL Configuration
    def get_postgres_config(self) -> PostgresConfig:
        return PostgresConfig(
            host=os.getenv('POSTGRES_HOST'),
            port=int(os.getenv('POSTGRES_PORT')),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            schema=os.getenv('POSTGRES_SCHEMA')
        )
    
    # Schema Management
    @lru_cache(maxsize=32)
    def load_avro_schema(self, schema_file: str) -> str:
        schema_path = self.schema_dir / schema_file
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")
        return schema_path.read_text(encoding='utf-8')
    
    @lru_cache(maxsize=1)
    def load_metadata(self) -> Dict[str, Any]:
        metadata_path = self.config_dir / "metadata.json"
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata not found: {metadata_path}")
        return json.loads(metadata_path.read_text(encoding='utf-8'))
    
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        metadata = self.load_metadata()
        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' not in metadata")
        return metadata[table_name]
    
    # Topic Configuration
    @staticmethod
    def get_topic_name(entity: str) -> str:
        prefix = os.getenv('TOPIC_PREFIX', '')
        return f"{prefix}.{entity}" if prefix else entity


# Global singleton instance
config = StreamingConfig()


# Convenience functions for backward compatibility
def get_producer_config(client_id: Optional[str] = None) -> Dict[str, Any]:
    return config.get_producer_config(client_id).to_dict()

def get_consumer_config(group_id: Optional[str] = None, 
                       client_id: Optional[str] = None) -> Dict[str, Any]:
    return config.get_consumer_config(group_id, client_id).to_dict()

def get_postgres_config() -> PostgresConfig:
    return config.get_postgres_config()

def load_avro_schema(schema_file: str) -> str:
    return config.load_avro_schema(schema_file)

def get_topic_name(entity: str) -> str:
    return config.get_topic_name(entity)