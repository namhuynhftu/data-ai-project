"""Quick script to check Kafka CDC messages."""
import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "debug-group",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["postgres.streaming.transactions"])

print("🔍 Checking for messages in postgres.streaming.transactions...")
count = 0
try:
    for _ in range(10):  # Check first 10 messages
        msg = consumer.poll(2.0)
        if not msg or msg.error():
            continue
        
        count += 1
        event = json.loads(msg.value().decode())
        print(f"\n📨 Message #{count}:")
        print(json.dumps(event, indent=2))
        
        if count >= 5:
            break
    
    if count == 0:
        print("❌ No messages found in topic")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"\n✅ Total messages checked: {count}")
