"""Query alerts from PostgreSQL database."""
import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="streaming_db",
    user="user",
    password="password"
)

cursor = conn.cursor()

# Get total alert count
cursor.execute("SELECT COUNT(*) FROM streaming.user_alerts")
total_alerts = cursor.fetchone()[0]
print(f"Total alerts in database: {total_alerts}\n")

if total_alerts > 0:
    # Get all alerts
    cursor.execute("""
        SELECT 
            alert_id,
            user_id,
            total_amount,
            transaction_count,
            alert_type,
            message,
            severity,
            detected_at,
            window_days,
            threshold_amount
        FROM streaming.user_alerts 
        ORDER BY detected_at DESC
    """)
    
    print("=" * 100)
    print("ALERT VIOLATIONS - Users Exceeding Transaction Thresholds")
    print("=" * 100)
    
    for row in cursor.fetchall():
        alert_id, user_id, total_amt, txn_count, alert_type, message, severity, detected_at, window_days, threshold = row
        print(f"\nAlert ID: {alert_id}")
        print(f"User ID: {user_id}")
        print(f"Total Amount: ${total_amt:,.2f}")
        print(f"Transaction Count: {txn_count}")
        print(f"Alert Type: {alert_type}")
        print(f"Severity: {severity}")
        print(f"Detection Time: {detected_at}")
        print(f"Window: {window_days} days | Threshold: ${threshold:,.2f}")
        print(f"Message: {message}")
        print("-" * 100)
    
    # Get summary statistics
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT user_id) as unique_users,
            AVG(total_amount) as avg_amount,
            MAX(total_amount) as max_amount,
            SUM(transaction_count) as total_transactions
        FROM streaming.user_alerts
    """)
    
    stats = cursor.fetchone()
    print(f"\nSUMMARY STATISTICS:")
    print(f"  Unique users violating threshold: {stats[0]}")
    print(f"  Average transaction amount: ${stats[1]:,.2f}")
    print(f"  Maximum transaction amount: ${stats[2]:,.2f}")
    print(f"  Total transactions: {stats[3]}")

cursor.close()
conn.close()
