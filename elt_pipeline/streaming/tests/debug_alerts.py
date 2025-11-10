"""Debug why only 1 alert is being generated."""
import sys
sys.path.insert(0, 'elt_pipeline/streaming/ops/processors')

from flink_cdc_monitoring import HighValueUserDetector
from confluent_kafka import Consumer
import json
from collections import defaultdict

detector = HighValueUserDetector()
detector.consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "debug-alerts",
    "auto.offset.reset": "earliest",
})
detector.consumer.subscribe(["postgres.streaming.transactions"])

print("Debugging alert generation...")
user_totals = defaultdict(float)
user_counts = defaultdict(int)
alerts_expected = []

count = 0
try:
    while count < 300:
        msg = detector.consumer.poll(2.0)
        if not msg or msg.error():
            continue
        
        count += 1
        event = json.loads(msg.value().decode())
        
        # Manually track what detector sees
        payload = event.get("payload", {})
        after = payload.get("after")
        if after:
            user_id = str(after.get("user_id"))
            
            import base64
            amount_bytes = after.get("amount", "")
            try:
                decoded = base64.b64decode(amount_bytes)
                value = int.from_bytes(decoded, byteorder='big', signed=True)
                amount = abs(value / 100.0)
                
                user_totals[user_id] += amount
                user_counts[user_id] += 1
                
                if user_totals[user_id] >= 3000 and user_id not in alerts_expected:
                    alerts_expected.append(user_id)
                    print(f"Message #{count}: User {user_id[:12]}... reached ${user_totals[user_id]:.2f} ({user_counts[user_id]} txns) - SHOULD ALERT")
            except:
                pass
        
        detector.process_transaction(event)
        
except KeyboardInterrupt:
    pass
finally:
    detector.consumer.close()
    detector.db_conn.close()
    
print(f"\nProcessed {count} messages")
print(f"\nExpected {len(alerts_expected)} alerts:")
for uid in alerts_expected:
    print(f"  {uid[:12]}...: ${user_totals[uid]:.2f}")

print(f"\nActually alerted: {len(detector.alerted_users)}")
for uid in detector.alerted_users:
    print(f"  {uid[:12]}...")
