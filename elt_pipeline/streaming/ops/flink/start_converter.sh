#!/bin/bash
# Start Kafka Avro-to-JSON Converter

echo "=== Starting Kafka Avro→JSON Converter ==="

# Install confluent-kafka if needed
echo "Installing confluent-kafka..."
docker exec flink_jobmanager pip install confluent-kafka[avro] --quiet 2>&1 | tail -5

# Copy converter script
echo "Copying converter to container..."
docker cp kafka_avro_to_json.py flink_jobmanager:/tmp/

# Check if JSON topic exists
echo "Checking for JSON topic..."
docker exec kafka_broker kafka-topics --bootstrap-server kafka:9092 --list | grep "postgres.streaming.transactions.json" || {
    echo "Creating JSON topic..."
    docker exec kafka_broker kafka-topics --bootstrap-server kafka:9092 --create --topic postgres.streaming.transactions.json --partitions 1 --replication-factor 1
}

# Run converter in background
echo "Starting converter process (background)..."
docker exec -d flink_jobmanager bash -c "cd /tmp && python3 kafka_avro_to_json.py > /tmp/converter.log 2>&1"

sleep 3

echo ""
echo "=== Converter Started ==="
echo "Monitor logs: docker exec flink_jobmanager tail -f /tmp/converter.log"
echo ""
echo "Wait 10 seconds for messages to convert, then run: bash submit_bucketed_job.sh"
