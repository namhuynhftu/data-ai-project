"""Flink CDC: Detect users with >=$3k transactions in last 300 days."""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HighValueUserDetector:
    """Track and alert on high-value user transactions."""

    def __init__(self):
        self.user_txns = defaultdict(list)
        self.threshold = 3000
        self.window_days = 300
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:29092",
            "group.id": "flink-monitor-group-v5",  # Changed to v5 to reprocess from beginning
            "auto.offset.reset": "earliest",
        })
        self.producer = Producer({"bootstrap.servers": "localhost:29092"})
        self.db_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="streaming_db",
            user="user",
            password="password"
        )
        self.alerted_users = set()  # Track users already alerted

    def _clean_old_txns(self, user_id: str):
        """Remove transactions older than window_days."""
        cutoff = datetime.utcnow() - timedelta(days=self.window_days)
        self.user_txns[user_id] = [
            txn for txn in self.user_txns[user_id] if txn["ts"] > cutoff
        ]

    def process_transaction(self, event: dict):
        """Process transaction CDC event and check threshold."""
        try:
            # Parse Debezium CDC event structure
            payload = event.get("payload", {})
            after = payload.get("after")
            
            if not after:
                return
            
            user_id = str(after.get("user_id"))
            
            # Decode amount from bytes (Debezium DECIMAL encoding)
            import base64
            amount_bytes = after.get("amount", "")
            try:
                decoded = base64.b64decode(amount_bytes)
                # Convert variable-length bytes to integer
                value = int.from_bytes(decoded, byteorder='big', signed=True)
                amount = abs(value / 100.0)  # scale=2
                logger.debug(f"Decoded amount: ${amount:.2f} for user {user_id}")
            except Exception as e:
                logger.error(f"Failed to decode amount '{amount_bytes}': {e}")
                return
            
            # Convert microsecond timestamp to datetime
            timestamp_us = after.get("timestamp", 0)
            ts = datetime.utcfromtimestamp(timestamp_us / 1_000_000)
            
            if not user_id or amount == 0:
                return

            txn = {"amount": amount, "ts": ts}
            self.user_txns[user_id].append(txn)
            self._clean_old_txns(user_id)

            total = sum(t["amount"] for t in self.user_txns[user_id])
            logger.debug(f"User {user_id[:8]}... total: ${total:.2f} (txns: {len(self.user_txns[user_id])})")
            
            # Check if user exceeds threshold
            if total >= self.threshold:
                # Query actual database for accurate totals
                cursor = self.db_conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*), COALESCE(SUM(ABS(amount)), 0)
                    FROM streaming.transactions 
                    WHERE user_id = %s 
                    AND timestamp >= NOW() - INTERVAL '%s days'
                """, (user_id, self.window_days))
                db_count, db_total = cursor.fetchone()
                cursor.close()
                
                alert = {
                    "user_id": user_id,
                    "total_amount": float(db_total),
                    "txn_count": db_count,
                    "alert_type": "HIGH_VALUE_USER",
                    "message": f"User {user_id} spent ${db_total:.2f} in last {self.window_days} days",
                    "severity": "HIGH",
                    "timestamp": datetime.utcnow().isoformat(),
                }
                
                # Only send alert if user not already alerted (prevents duplicates)
                if user_id not in self.alerted_users:
                    self._send_alert(alert)
                    self.alerted_users.add(user_id)
                    logger.warning(f"🚨 HIGH VALUE: {user_id} = ${db_total:.2f} ({db_count} txns from DB)")
        except Exception as e:
            logger.error(f"Process error: {e}")

    def _send_alert(self, alert: dict):
        """Send alert to Kafka and PostgreSQL."""
        # Send to Kafka (optional - skip if topic doesn't exist)
        try:
            self.producer.produce(
                "user-alerts",
                key=alert["user_id"].encode(),
                value=json.dumps(alert).encode(),
            )
            self.producer.flush()
        except Exception as kafka_err:
            logger.warning(f"Kafka alert failed (will still save to DB): {kafka_err}")
        
        # Insert into PostgreSQL
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO streaming.user_alerts 
                (user_id, total_amount, transaction_count, alert_type, message, 
                 severity, detected_at, window_days, threshold_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                alert["user_id"],
                alert["total_amount"],
                alert["txn_count"],
                alert["alert_type"],
                alert["message"],
                alert["severity"],
                datetime.utcnow(),
                self.window_days,
                self.threshold
            ))
            self.db_conn.commit()
            cursor.close()
            logger.info(f"✅ Alert saved to database for user {alert['user_id'][:8]}...")
        except Exception as e:
            logger.error(f"Failed to save alert to database: {e}")
            self.db_conn.rollback()

    def run(self):
        """Consume transactions and monitor continuously."""
        logger.info(f"🚀 Monitoring users with >={self.threshold} in {self.window_days} days")
        self.consumer.subscribe(["postgres.streaming.transactions"])

        try:
            msg_count = 0
            while True:
                msg = self.consumer.poll(1.0)
                if not msg or msg.error():
                    continue

                msg_count += 1
                event = json.loads(msg.value().decode())
                logger.info(f"📨 Message #{msg_count} received")
                self.process_transaction(event)
        except KeyboardInterrupt:
            logger.info("⏹️  Stopped")
        finally:
            self.consumer.close()
            self.producer.flush()
            self.db_conn.close()


if __name__ == "__main__":
    detector = HighValueUserDetector()
    detector.run()
