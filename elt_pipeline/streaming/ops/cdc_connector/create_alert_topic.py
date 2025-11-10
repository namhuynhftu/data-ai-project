"""Create user-alerts Kafka topic."""
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:29092"})

topic = NewTopic(
    topic="user-alerts",
    num_partitions=1,
    replication_factor=1
)

try:
    futures = admin.create_topics([topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"✅ Topic '{topic_name}' created successfully")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"ℹ️  Topic '{topic_name}' already exists")
            else:
                print(f"❌ Failed to create topic '{topic_name}': {e}")
except Exception as e:
    print(f"Error: {e}")
