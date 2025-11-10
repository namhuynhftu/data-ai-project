"""Run Flink monitoring and verify all alerts are saved."""
import subprocess
import time
import psycopg2

print("Starting Flink CDC monitoring (processing all 300 messages)...")
print("This will take about 15-20 seconds...")

# Run the monitoring script in background
proc = subprocess.Popen(
    ["uv", "run", "python", "elt_pipeline/streaming/ops/processors/flink_cdc_monitoring.py"],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True
)

# Wait for it to process messages
time.sleep(18)

# Stop the process
proc.terminate()
time.sleep(2)

# Check results
conn = psycopg2.connect(
    host="localhost", port=5432, database="streaming_db",
    user="user", password="password"
)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM streaming.user_alerts")
alert_count = cursor.fetchone()[0]

print(f"\n{'='*70}")
print(f"RESULTS:")
print(f"{'='*70}")
print(f"Alerts saved to database: {alert_count}")

if alert_count > 0:
    cursor.execute("""
        SELECT user_id, total_amount, transaction_count, severity 
        FROM streaming.user_alerts 
        ORDER BY total_amount DESC
    """)
    print(f"\nAlerts in database:")
    for row in cursor.fetchall():
        print(f"  User {str(row[0])[:12]}...: ${row[1]:,.2f} ({row[2]} txns) - {row[3]}")

# Compare with expected
cursor.execute("""
    SELECT COUNT(*) 
    FROM (
        SELECT user_id, SUM(ABS(amount)) as total 
        FROM streaming.transactions 
        GROUP BY user_id 
        HAVING SUM(ABS(amount)) >= 3000
    ) t
""")
expected_count = cursor.fetchone()[0]

print(f"\nExpected alerts (users >= $3,000): {expected_count}")

if alert_count == expected_count:
    print(f"\n✅ SUCCESS! All {expected_count} users properly alerted")
else:
    print(f"\n❌ MISMATCH! Expected {expected_count} but got {alert_count}")
    print(f"   Missing {expected_count - alert_count} alerts")

cursor.close()
conn.close()
