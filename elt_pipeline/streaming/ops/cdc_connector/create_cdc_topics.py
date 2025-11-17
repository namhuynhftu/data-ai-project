import subprocess
import sys
import time
from typing import Dict, List
import os

from elt_pipeline.streaming.ops.cdc_connector.kafka_topic_manager import KafkaTopicManager

def main():
    """Create CDC topics for PostgreSQL streaming tables."""
    manager = KafkaTopicManager()

    cdc_topics = [
        "postgres.streaming.users",
        "postgres.streaming.transactions",
        "postgres.streaming.detailed_transactions",
    ]

    print("Creating CDC topics for Debezium...")
    print("=" * 60)

    success_count = 0
    success_topic_creation = []
    for topic in cdc_topics:
        if manager.create_topic(topic, partitions=3, replication=1):
            success_count += 1
            success_topic_creation.append(topic)
        time.sleep(0.5)
        
        

    print("=" * 60)
    print(
        f"Summary: {success_count}/{len(cdc_topics)} topics created"
    )

    if success_count == len(cdc_topics):
        print("All CDC topics ready!")
        return 0
    print("Some topics failed to create")
    return 1


if __name__ == "__main__":
    sys.exit(main())
