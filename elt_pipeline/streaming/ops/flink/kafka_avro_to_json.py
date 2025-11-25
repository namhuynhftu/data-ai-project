"""
Kafka Avro-to-JSON Converter
Reads Debezium Avro messages and publishes as JSON to new topic

Run this first to convert postgres.streaming.transactions (Avro) 
to postgres.streaming.transactions.json (JSON)
"""

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import json
import time

# Configuration
SOURCE_TOPIC = "postgres.streaming.transactions"
TARGET_TOPIC = "postgres.streaming.transactions.json"
KAFKA_BOOTSTRAP = "kafka:9092"
SCHEMA_REGISTRY = "http://schema-registry:8081"
CONSUMER_GROUP = "avro-to-json-converter"


def create_avro_consumer():
    """Create Confluent Avro consumer"""
    return AvroConsumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': CONSUMER_GROUP,
        'schema.registry.url': SCHEMA_REGISTRY,
        'auto.offset.reset': 'earliest'
    })


def create_json_producer():
    """Create JSON producer"""
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'compression.type': 'snappy'
    })


def convert_avro_to_json(avro_msg):
    """
    Convert Debezium Avro message to simplified JSON format.
    
    Input: Full Debezium envelope with before/after
    Output: Simplified {user_id, amount, timestamp_ms, op}
    """
    try:
        # Extract operation type
        op = avro_msg.get('op', '')
        
        # Only process create/update/read operations
        if op not in ('c', 'u', 'r'):
            return None
        
        # Extract 'after' state
        after = avro_msg.get('after')
        if after is None:
            return None
        
        # Build simplified JSON
        json_msg = {
            'user_id': after.get('user_id'),
            'amount': after.get('amount'),
            'timestamp_micros': after.get('timestamp'),
            'currency': after.get('currency'),
            'op': op
        }
        
        # Validate required fields
        if not all([json_msg['user_id'], json_msg['amount'], json_msg['timestamp_micros']]):
            return None
        
        return json_msg
    
    except Exception as e:
        print(f"Error converting message: {e}")
        return None


def delivery_report(err, msg):
    """Callback for producer delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def main():
    """Main converter loop"""
    consumer = create_avro_consumer()
    producer = create_json_producer()
    
    # Subscribe to source topic
    consumer.subscribe([SOURCE_TOPIC])
    
    print("="*70)
    print("Kafka Avro-to-JSON Converter Started")
    print("="*70)
    print(f"Source Topic (Avro):  {SOURCE_TOPIC}")
    print(f"Target Topic (JSON):  {TARGET_TOPIC}")
    print(f"Schema Registry:      {SCHEMA_REGISTRY}")
    print(f"Consumer Group:       {CONSUMER_GROUP}")
    print("="*70)
    print("\nListening for messages... (Press Ctrl+C to stop)\n")
    
    msg_count = 0
    error_count = 0
    
    try:
        while True:
            # Poll for messages (timeout 1 second)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition {msg.partition()}')
                else:
                    print(f'Error: {msg.error()}')
                    error_count += 1
                continue
            
            # Message successfully received
            avro_value = msg.value()
            
            # Convert to JSON
            json_msg = convert_avro_to_json(avro_value)
            
            if json_msg:
                # Serialize to JSON string
                json_str = json.dumps(json_msg)
                
                # Publish to target topic
                key_bytes = msg.key().encode('utf-8') if msg.key() and isinstance(msg.key(), str) else None
                producer.produce(
                    topic=TARGET_TOPIC,
                    key=key_bytes,  # Preserve original key for partitioning
                    value=json_str.encode('utf-8')
                )
                
                msg_count += 1
                
                if msg_count % 100 == 0:
                    print(f"Converted {msg_count} messages (errors: {error_count})")
                    producer.flush()
            
            # Flush every 1000 messages
            if msg_count % 1000 == 0:
                producer.flush()
    
    except KeyboardInterrupt:
        print("\n\nShutting down converter...")
    
    finally:
        # Flush remaining messages
        producer.flush()
        consumer.close()
        
        print("\n" + "="*70)
        print(f"Converter stopped")
        print(f"Total messages converted: {msg_count}")
        print(f"Total errors: {error_count}")
        print("="*70)


if __name__ == "__main__":
    main()
