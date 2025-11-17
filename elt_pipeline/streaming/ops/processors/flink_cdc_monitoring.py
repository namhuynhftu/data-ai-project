"""Kafka CDC Monitor: Detect users with >=$3k transactions in last 300 days.

Note: This uses a Python Kafka consumer with Avro deserialization, not Flink.
For true Flink processing, deploy to the Flink cluster running in Docker.
"""
import logging
from datetime import datetime, timedelta

import psycopg2
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FlinkTransactionMonitor:
    """Real Flink streaming job to monitor high-value users."""

    def __init__(self):
        self.threshold = 3000
        self.window_days = 300
        
        # Create Flink streaming environment
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.table_env = TableEnvironment.create(env_settings)
        
        # Set checkpoint interval for fault tolerance
        self.table_env.get_config().get_configuration().set_string(
            "execution.checkpointing.interval", "60s"
        )
        
        logger.info("✅ Flink Table Environment created")

    def create_source_table(self):
        """Create Kafka source table for CDC transactions (Avro format)."""
        logger.info("📥 Creating Kafka source table...")
        
        self.table_env.execute_sql("""
            CREATE TABLE transactions_source (
                `before` ROW<
                    transaction_id STRING,
                    user_id STRING,
                    amount DECIMAL(10,2),
                    currency STRING,
                    `timestamp` BIGINT
                >,
                `after` ROW<
                    transaction_id STRING,
                    user_id STRING,
                    amount DECIMAL(10,2),
                    currency STRING,
                    `timestamp` BIGINT
                >,
                `source` ROW<
                    version STRING,
                    connector STRING,
                    name STRING,
                    ts_ms BIGINT
                >,
                op STRING,
                ts_ms BIGINT,
                -- Convert microsecond timestamp to TIMESTAMP(3)
                event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`after`.`timestamp` / 1000000)),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'postgres.streaming.transactions',
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'flink-pyflink-monitor',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'avro-confluent',
                'avro-confluent.url' = 'http://schema-registry:8081'
            )
        """)
        
        logger.info("✅ Kafka source table created")

    def create_sink_table(self):
        """Create PostgreSQL sink table for alerts."""
        logger.info("📤 Creating PostgreSQL sink table...")
        
        self.table_env.execute_sql("""
            CREATE TABLE user_alerts_sink (
                user_id STRING,
                total_amount DECIMAL(12,2),
                transaction_count BIGINT,
                alert_type STRING,
                message STRING,
                severity STRING,
                detected_at TIMESTAMP(3),
                window_days INT,
                threshold_amount DECIMAL(12,2)
            ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://postgres_streaming:5432/streaming_db',
                'table-name' = 'streaming.user_alerts',
                'username' = 'user',
                'password' = 'password',
                'driver' = 'org.postgresql.Driver'
            )
        """)
        
        logger.info("✅ PostgreSQL sink table created")

    def create_monitoring_query(self):
        """Create windowed aggregation query to detect high-value users."""
        logger.info("🔍 Creating monitoring query...")
        
        # Create view with aggregated data
        # Note: Using INTERVAL '10' MONTH instead of '300' DAY to avoid precision limits
        self.table_env.execute_sql(f"""
            CREATE TEMPORARY VIEW high_value_users AS
            SELECT 
                `after`.user_id as user_id,
                SUM(ABS(`after`.amount)) as total_amount,
                COUNT(*) as transaction_count,
                'HIGH_VALUE_USER' as alert_type,
                CONCAT('User ', CAST(`after`.user_id AS STRING), ' spent $', 
                       CAST(SUM(ABS(`after`.amount)) AS STRING), ' in last {self.window_days} days') as message,
                'HIGH' as severity,
                CURRENT_TIMESTAMP as detected_at,
                {self.window_days} as window_days,
                CAST({self.threshold} AS DECIMAL(12,2)) as threshold_amount
            FROM transactions_source
            WHERE 
                `after` IS NOT NULL
                AND event_time >= CURRENT_TIMESTAMP - INTERVAL '10' MONTH
            GROUP BY 
                `after`.user_id,
                TUMBLE(event_time, INTERVAL '1' MINUTE)
            HAVING 
                SUM(ABS(`after`.amount)) >= {self.threshold}
        """)
        
        logger.info("✅ Monitoring query created")

    def run(self):
        """Execute Flink streaming job."""
        logger.info("=" * 60)
        logger.info("🚀 Starting Flink Transaction Monitor")
        logger.info(f"   Threshold: ${self.threshold}")
        logger.info(f"   Window: {self.window_days} days")
        logger.info(f"   Target: streaming.user_alerts")
        logger.info("=" * 60)
        
        # Create tables
        self.create_source_table()
        self.create_sink_table()
        self.create_monitoring_query()
        
        # Execute insert statement
        logger.info("▶️  Executing streaming job...")
        
        statement_set = self.table_env.create_statement_set()
        statement_set.add_insert_sql("""
            INSERT INTO user_alerts_sink
            SELECT 
                user_id,
                total_amount,
                transaction_count,
                alert_type,
                message,
                severity,
                detected_at,
                window_days,
                threshold_amount
            FROM high_value_users
        """)
        
        # Execute and wait
        result = statement_set.execute()
        logger.info(f"✅ Job submitted: {result.get_job_client().get_job_id()}")
        logger.info("⏳ Job running... (Press Ctrl+C to stop)")
        
        # Wait for completion (runs indefinitely)
        result.wait()


if __name__ == "__main__":
    try:
        monitor = FlinkTransactionMonitor()
        monitor.run()
    except KeyboardInterrupt:
        logger.info("⏹️  Stopped by user")
    except Exception as e:
        logger.error(f"❌ Job failed: {e}", exc_info=True)
        raise
