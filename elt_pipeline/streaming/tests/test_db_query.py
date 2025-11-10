"""Test updated Flink monitoring with DB query."""
import sys
sys.path.insert(0, 'elt_pipeline/streaming/ops/processors')

from flink_cdc_monitoring import HighValueUserDetector
from confluent_kafka import Consumer
import json

detector = HighValueUserDetector()
detector.consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "flink-test-db-query",
    "auto.offset.reset": "earliest",
})
detector.consumer.subscribe(["postgres.streaming.transactions"])

print("Testing with DB query for accurate totals...")
count = 0
try:
    while count < 200:
        msg = detector.consumer.poll(2.0)
        if not msg or msg.error():
            continue
        
        count += 1
        event = json.loads(msg.value().decode())
        if count % 50 == 0:
            print(f"Processed {count} messages...")
        detector.process_transaction(event)
        
except KeyboardInterrupt:
    pass
finally:
    detector.consumer.close()
    detector.db_conn.close()
    print(f"\nProcessed {count} messages")
    
    # Check results
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="streaming_db",
        user="user", password="password"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, total_amount, transaction_count FROM streaming.user_alerts ORDER BY detected_at DESC LIMIT 1")
    result = cursor.fetchone()
    if result:
        print(f"\nAlert saved:")
        print(f"  User: {result[0]}")
        print(f"  Total from alert: ${result[1]:.2f}")
        print(f"  Transactions: {result[2]}")
        
        # Compare with actual DB
        cursor.execute("SELECT COUNT(*), SUM(ABS(amount)) FROM streaming.transactions WHERE user_id = %s", (result[0],))
        actual = cursor.fetchone()
        print(f"\nActual in transactions table:")
        print(f"  Transactions: {actual[0]}")
        print(f"  Total: ${actual[1]:.2f}")
        
        if result[1] == actual[1]:
            print("\n✅ MATCH! Alert shows correct database total")
        else:
            print(f"\n❌ MISMATCH! Difference: ${abs(result[1] - actual[1]):.2f}")
    
    cursor.close()
    conn.close()
