"""Count total messages in Kafka topic."""
from confluent_kafka import Consumer
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "count-messages-test",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["postgres.streaming.transactions"])

print("Counting messages in postgres.streaming.transactions topic...")
count = 0
timeout_count = 0
max_timeout = 5

try:
    while timeout_count < max_timeout:
        msg = consumer.poll(1.0)
        if not msg:
            timeout_count += 1
            continue
        if msg.error():
            continue
        
        count += 1
        timeout_count = 0  # Reset on successful message
        
        if count % 50 == 0:
            print(f"  {count} messages...")
            
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"\nTotal messages in Kafka topic: {count}")
