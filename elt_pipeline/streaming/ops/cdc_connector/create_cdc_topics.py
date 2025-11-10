"""
Create Kafka topics for CDC data ingestion.

This script pre-creates topics required by Debezium CDC connector
since Kafka has auto.create.topics.enable=false for production safety.
"""

import subprocess
import sys
import time
from typing import Dict, List


class KafkaTopicManager:
    """Manage Kafka topic creation for CDC pipeline."""

    def __init__(self, bootstrap_server: str = "localhost:29092"):
        self.bootstrap_server = bootstrap_server
        self.kafka_cmd = self._get_kafka_cmd()

    def _get_kafka_cmd(self) -> str:
        """Get platform-specific Kafka CLI command."""
        return "docker exec kafka_broker /opt/kafka/bin/kafka-topics.sh"

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
                print(f"✅ Created topic: {topic_name}")
                return True
            if "already exists" in result.stderr.lower():
                print(f"ℹ️  Topic already exists: {topic_name}")
                return True
            print(
                f"❌ Failed to create {topic_name}: "
                f"{result.stderr.strip()}"
            )
            return False
        except Exception as e:
            print(f"❌ Error creating topic {topic_name}: {e}")
            return False

    def list_topics(self) -> List[str]:
        """List all existing topics."""
        cmd = (
            f"{self.kafka_cmd} --bootstrap-server "
            f"{self.bootstrap_server} --list"
        )
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, check=True
            )
            return result.stdout.strip().split("\n")
        except Exception as e:
            print(f"❌ Error listing topics: {e}")
            return []


def main():
    """Create CDC topics for PostgreSQL streaming tables."""
    manager = KafkaTopicManager()

    # CDC topics follow pattern: <topic.prefix>.<schema>.<table>
    cdc_topics = [
        "postgres.streaming.users",
        "postgres.streaming.transactions",
        "postgres.streaming.detailed_transactions",
    ]

    print("📋 Creating CDC topics for Debezium...")
    print("=" * 60)

    success_count = 0
    for topic in cdc_topics:
        if manager.create_topic(topic, partitions=3, replication=1):
            success_count += 1
        time.sleep(0.5)

    print("=" * 60)
    print(
        f"\n📊 Summary: {success_count}/{len(cdc_topics)} topics created"
    )

    if success_count == len(cdc_topics):
        print("✅ All CDC topics ready!")
        return 0
    print("⚠️  Some topics failed to create")
    return 1


if __name__ == "__main__":
    sys.exit(main())
