"""Kafka CDC Monitor: Detect users with >=$3k transactions in last 300 days.

This uses a Python Kafka consumer with Avro deserialization.
For true Flink processing, deploy a Flink SQL job to the cluster in Docker.
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionMonitor:
    """Monitor high-value user transactions using Kafka CDC events."""

    def __init__(self):
        self.user_txns = defaultdict(list)
        self.threshold = 3000
        self.window_days = 300
        
        # Schema Registry client
        self.schema_registry = SchemaRegistryClient({"url": "http://localhost:8081"})
        
        # Avro deserializer
        self.avro_deserializer = AvroDeserializer(
            self.schema_registry,
            schema_str=None  # Auto-fetch from registry
        )
        
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:29092",
            "group.id": "transaction-monitor-v1",
            "auto.offset.reset": "earliest",
        })
        
        self.db_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="streaming_db",
            user="user",
            password="password"
        )
        
        self.alerted_users = set()  # Track users already alerted
        
        logger.info(f"Monitor initialized (threshold=${self.threshold}, window={self.window_days}d)")

    def _clean_old_txns(self, user_id: str):
        """Remove transactions older than window_days."""
        cutoff = datetime.utcnow() - timedelta(days=self.window_days)
        self.user_txns[user_id] = [
            txn for txn in self.user_txns[user_id] if txn["ts"] > cutoff
        ]

    def process_transaction(self, event: dict):
        """Process transaction CDC event and check threshold."""
        try:
            # Event structure: {"before": {...}, "after": {...}, "source": {...}, "op": "c/u/d"}
            after = event.get("after")
            
            if not after:
                return
            
            user_id = str(after.get("user_id"))
            amount = abs(float(after.get("amount", 0)))  # Avro deserializes DECIMAL to float
            
            # Convert microsecond timestamp to datetime
            timestamp_us = after.get("timestamp", 0)
            ts = datetime.utcfromtimestamp(timestamp_us / 1_000_000)
            
            if not user_id or amount == 0:
                return

            txn = {"amount": amount, "ts": ts}
            self.user_txns[user_id].append(txn)
            self._clean_old_txns(user_id)

            total = sum(t["amount"] for t in self.user_txns[user_id])
            
            # Check if user exceeds threshold
            if total >= self.threshold and user_id not in self.alerted_users:
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
                
                # Insert alert to database
                self._save_alert(user_id, float(db_total), db_count)
                self.alerted_users.add(user_id)
                logger.warning(f"HIGH VALUE: {user_id[:8]}... = ${db_total:.2f} ({db_count} txns)")
                
        except Exception as e:
            logger.error(f"Process error: {e}")

    def _save_alert(self, user_id: str, total_amount: float, txn_count: int):
        """Save alert to PostgreSQL."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO streaming.user_alerts 
                (user_id, total_amount, transaction_count, alert_type, message, 
                 severity, detected_at, window_days, threshold_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                total_amount,
                txn_count,
                'HIGH_VALUE_USER',
                f'User {user_id} spent ${total_amount:.2f} in last {self.window_days} days',
                'HIGH',
                datetime.utcnow(),
                self.window_days,
                self.threshold
            ))
            self.db_conn.commit()
            cursor.close()
            logger.info(f"Alert saved for user {user_id[:8]}...")
        except Exception as e:
            logger.error(f"Failed to save alert: {e}")
            self.db_conn.rollback()

    def run(self):
        """Consume transactions and monitor continuously."""
        logger.info("=" * 60)
        logger.info(f"Monitoring users with >=${self.threshold} in {self.window_days} days")
        logger.info("=" * 60)
        
        self.consumer.subscribe(["postgres.streaming.transactions"])

        try:
            msg_count = 0
            while True:
                msg = self.consumer.poll(1.0)
                if not msg or msg.error():
                    continue

                msg_count += 1
                
                # Deserialize Avro message
                ctx = SerializationContext("postgres.streaming.transactions", MessageField.VALUE)
                event = self.avro_deserializer(msg.value(), ctx)
                
                if event:
                    if msg_count % 10 == 0:  # Log every 10th message
                        logger.info(f"Processed {msg_count} messages")
                    self.process_transaction(event)
                    
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            self.consumer.close()
            self.db_conn.close()
            logger.info(f"Total messages processed: {msg_count}")


if __name__ == "__main__":
    monitor = TransactionMonitor()
    monitor.run()
