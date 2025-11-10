"""Test Flink monitoring with PostgreSQL integration."""
import sys
sys.path.insert(0, 'elt_pipeline/streaming/ops/processors')

from flink_cdc_monitoring import HighValueUserDetector
from confluent_kafka import Consumer
import json

detector = HighValueUserDetector()
detector.consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "flink-test-postgres",
    "auto.offset.reset": "earliest",
})
detector.consumer.subscribe(["postgres.streaming.transactions"])

print("Testing Flink CDC monitoring with PostgreSQL integration...")
count = 0
try:
    while count < 300:
        msg = detector.consumer.poll(2.0)
        if not msg or msg.error():
            continue
        
        count += 1
        event = json.loads(msg.value().decode())
        if count % 20 == 0:
            print(f"Processed {count} messages...")
        detector.process_transaction(event)
        
except KeyboardInterrupt:
    pass
finally:
    detector.consumer.close()
    detector.db_conn.close()
    print(f"\nProcessed {count} messages")
    print(f"Alerted users: {len(detector.alerted_users)}")
    
    # Check database for saved alerts
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="streaming_db",
        user="user", password="password"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM streaming.user_alerts")
    alert_count = cursor.fetchone()[0]
    print(f"Alerts in database: {alert_count}")
    
    if alert_count > 0:
        cursor.execute("""
            SELECT user_id, total_amount, transaction_count, severity, detected_at 
            FROM streaming.user_alerts 
            ORDER BY detected_at DESC LIMIT 5
        """)
        print("\nRecent alerts:")
        for row in cursor.fetchall():
            print(f"  User {str(row[0])[:12]}...: ${row[1]:.2f} ({row[2]} txns) - {row[3]} at {row[4]}")
    
    cursor.close()
    conn.close()
