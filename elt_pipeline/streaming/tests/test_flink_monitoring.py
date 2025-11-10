"""Test Flink monitoring with detailed output."""
import sys
sys.path.insert(0, 'elt_pipeline/streaming/ops/processors')

from flink_cdc_monitoring import HighValueUserDetector
from confluent_kafka import Consumer

detector = HighValueUserDetector()
detector.consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "flink-test-debug",
    "auto.offset.reset": "earliest",
})
detector.consumer.subscribe(["postgres.streaming.transactions"])

print("Testing Flink CDC monitoring with detailed logging...")
count = 0
try:
    while count < 200:
        msg = detector.consumer.poll(2.0)
        if not msg or msg.error():
            continue
        
        count += 1
        import json
        event = json.loads(msg.value().decode())
        print(f"\nProcessing message #{count}")
        detector.process_transaction(event)
        
except KeyboardInterrupt:
    pass
finally:
    detector.consumer.close()
    print(f"\nProcessed {count} messages")
    print(f"Tracked {len(detector.user_txns)} unique users")
    if detector.user_txns:
        # Sort by total amount
        user_totals = [(uid, sum(t["amount"] for t in txns), len(txns)) 
                       for uid, txns in detector.user_txns.items()]
        user_totals.sort(key=lambda x: x[1], reverse=True)
        
        print("\nTop 10 users by total amount:")
        for user_id, total, txn_count in user_totals[:10]:
            status = "ALERT!" if total >= 3000 else ""
            print(f"   User {user_id[:12]}...: ${total:.2f} ({txn_count} txns) {status}")
