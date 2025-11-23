

import subprocess
import sys
import time
from typing import Dict, List
import os

class KafkaTopicManager:
    """Manage Kafka topic creation for CDC pipeline."""

    def __init__(self, bootstrap_server: str = "kafka:9092"):
        # Use internal Docker network address (kafka:9092) since we exec into container
        self.bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVERS', bootstrap_server)
        self.kafka_cmd = self._get_kafka_cmd()

    def _get_kafka_cmd(self) -> str:
        """Get platform-specific Kafka CLI command."""
        # Try to detect the Kafka container name
        container_name = os.getenv('KAFKA_CONTAINER', 'kafka_broker')
        return f"docker exec {container_name} kafka-topics"

    def create_topic(
        self, topic_name: str, partitions: int = 3, replication: int = 1
    ) -> bool:
        """Create a single Kafka topic."""
        cmd = (
            f"{self.kafka_cmd} --bootstrap-server {self.bootstrap_server} "
            f"--create --topic {topic_name} "
            f"--partitions {partitions} --replication-factor {replication}"
        )

        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, check=False
            )
            if result.returncode == 0:
                print(f"Created topic: {topic_name}")
                return True
            if "already exists" in result.stderr.lower():
                print(f"Topic already exists: {topic_name}")
                return True
            print(f"Failed to create {topic_name}: {result.stderr.strip()}")
            return False
        except Exception as e:
            print(f"Error creating topic {topic_name}: {e}")
            return False
    
    def delete_topic(self, topic_name: str) -> bool:
        """Delete a single Kafka topic."""
        cmd = (
            f"{self.kafka_cmd} --bootstrap-server {self.bootstrap_server} "
            f"--delete --topic {topic_name}"
        )

        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, check=False
            )
            if result.returncode == 0:
                print(f"Deleted topic: {topic_name}")
                return True
            print(f"Failed to delete {topic_name}: {result.stderr.strip()}")
            return False
        except Exception as e:
            print(f"Error deleting topic {topic_name}: {e}")
            return False
