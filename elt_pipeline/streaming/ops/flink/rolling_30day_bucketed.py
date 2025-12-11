"""
PyFlink DataStream Job: Real-time 30-Day Rolling Sum with Bucketed State
Architecture: Debezium CDC → Kafka → PyFlink → PostgreSQL

Uses daily buckets to efficiently maintain rolling 30-day windows per user.
Emits alerts immediately when user spending crosses $3000 threshold.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common import Types, WatermarkStrategy, Duration, Row
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types as TypeInfo
import json
from datetime import datetime, timedelta
from typing import Tuple, Dict
import io


# ============================================================================
# CONFIGURATION
# ============================================================================
WINDOW_SIZE_DAYS = 30
THRESHOLD_AMOUNT = 3000.00
BUCKET_SIZE_DAYS = 1  # Daily buckets (30 buckets total)
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "postgres.streaming.transactions.json"  # JSON topic from converter
KAFKA_GROUP_ID = "pyflink-bucketed-30day-v5"  # New group to start fresh
POSTGRES_URL = "jdbc:postgresql://postgres_streaming:5432/streaming_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"


# ============================================================================
# BUCKETED 30-DAY ROLLING SUM PROCESS FUNCTION
# ============================================================================
class BucketedRolling30DaySum(KeyedProcessFunction):
    """
    Maintains per-user state with daily buckets for 30-day rolling window.
    """
    
    def __init__(self):
        self.daily_buckets = None
        self.last_alert_sum = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state descriptors"""
        # MapState: key = day_number (epoch_days), value = sum for that day
        bucket_desc = MapStateDescriptor(
            "daily_buckets",
            Types.INT(),      # day number since epoch
            Types.DOUBLE()    # sum of amounts for that day
        )
        self.daily_buckets = runtime_context.get_map_state(bucket_desc)
        
        # ValueState: tracks the last sum that triggered an alert
        last_alert_desc = ValueStateDescriptor(
            "last_alert_sum",
            Types.DOUBLE()
        )
        self.last_alert_sum = runtime_context.get_state(last_alert_desc)
    
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """
        Process each transaction event.
        
        Args:
            value: Tuple of (user_id, amount, event_time_ms)
            ctx: KeyedProcessFunction context
        """
        user_id = value[0]
        amount = abs(float(value[1]))  # Use absolute value
        event_time_ms = int(value[2])
        
        # Calculate which day bucket this transaction belongs to
        event_day = self._get_day_number(event_time_ms)
        
        # Add amount to the appropriate day bucket
        current_bucket_sum = self.daily_buckets.get(event_day)
        if current_bucket_sum is None:
            current_bucket_sum = 0.0
        self.daily_buckets.put(event_day, current_bucket_sum + amount)
        
        # Remove buckets older than 30 days
        cutoff_day = event_day - WINDOW_SIZE_DAYS
        self._cleanup_old_buckets(cutoff_day)
        
        # Calculate total sum only for buckets in the 30-day window
        # (from cutoff_day to event_day, not including future events)
        total_sum = 0.0
        bucket_count = 0
        for bucket_day, bucket_sum in self.daily_buckets.items():
            if cutoff_day <= bucket_day <= event_day:
                total_sum += bucket_sum
                bucket_count += 1
        
        # Emit alert if threshold crossed
        if total_sum >= THRESHOLD_AMOUNT:
            last_sum = self.last_alert_sum.value()
            
            # Only emit if sum changed (avoid duplicate alerts)
            if last_sum is None or abs(total_sum - last_sum) > 0.01:
                self.last_alert_sum.update(total_sum)
                
                # Calculate window boundaries
                window_start_ms = (cutoff_day + 1) * 86400000  # Convert day to ms
                window_end_ms = event_time_ms
                total_rounded = round(total_sum, 2)
                
                # Emit alert as Row object matching JDBC sink parameters:
                # (user_id, total_amount, tx_count, total_amount_for_message, window_end_ms)
                yield Row(
                    user_id,
                    total_rounded,
                    bucket_count,
                    total_rounded,  # Duplicate for message CONCAT
                    window_end_ms
                )
        elif self.last_alert_sum.value() is not None:
            # User dropped below threshold - clear alert state
            self.last_alert_sum.clear()
    
    def _get_day_number(self, timestamp_ms: int) -> int:
        """Convert timestamp to day number (days since epoch)"""
        return int(timestamp_ms // 86400000)  # 86400000 ms = 1 day
    
    def _cleanup_old_buckets(self, cutoff_day: int):
        """Remove buckets older than cutoff_day"""
        keys_to_remove = []
        for day_num in self.daily_buckets.keys():
            if day_num < cutoff_day:
                keys_to_remove.append(day_num)
        
        for day_num in keys_to_remove:
            self.daily_buckets.remove(day_num)


# ============================================================================
# JSON DESERIALIZATION
# ============================================================================
def parse_json_message(json_str: str) -> Tuple:
    """
    Parse JSON message from Kafka (converted from Debezium Avro).
    
    Expected JSON format:
    {
      "user_id": "12345678-1234-1234-1234-123456789abe",
      "amount": "2600.00",
      "timestamp_micros": 1732454400000000,
      "currency": "USD",
      "op": "c"
    }
    
    Returns:
        Tuple of (user_id, amount, event_time_ms) or None if invalid
    """
    try:
        record = json.loads(json_str)
        
        user_id = record.get('user_id')
        amount_str = record.get('amount')
        timestamp_micros = record.get('timestamp_micros')
        
        if not all([user_id, amount_str, timestamp_micros]):
            return None
        
        # Convert microseconds to milliseconds
        event_time_ms = int(timestamp_micros) // 1000
        
        return (user_id, amount_str, event_time_ms)
    
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        return None


# ============================================================================
# TIMESTAMP ASSIGNER FOR WATERMARKS
# ============================================================================
class TransactionTimestampAssigner(TimestampAssigner):
    """Extract event timestamp from tuple for watermark generation"""
    
    def extract_timestamp(self, value, record_timestamp):
        """Value is (user_id, amount, event_time_ms)"""
        return value[2]  # event_time_ms


# ============================================================================
# KAFKA SOURCE
# ============================================================================
def create_kafka_source() -> KafkaSource:
    """
    Create Kafka source for JSON messages.
    
    Reads from postgres.streaming.transactions.json topic which contains
    JSON-serialized Debezium messages (converted from Avro).
    """
    return KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id(KAFKA_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


# ============================================================================
# JDBC SINK FOR POSTGRESQL
# ============================================================================
def create_jdbc_sink() -> JdbcSink:
    """
    Create JDBC sink to write alerts to PostgreSQL.
    
    Table: streaming.user_alerts
    Schema: (user_id, total_amount, transaction_count, alert_type, message, 
             severity, detected_at, window_days, threshold_amount, created_at, updated_at)
    """
    return JdbcSink.sink(
        """
        INSERT INTO streaming.user_alerts 
        (user_id, total_amount, transaction_count, alert_type, message, 
         severity, detected_at, window_days, threshold_amount, created_at, updated_at)
        VALUES (?, ?, ?, 'HIGH_VALUE_USER', 
                CONCAT('User spent $', CAST(? AS VARCHAR), ' in last 30 days'),
                'HIGH', to_timestamp(? / 1000), 30, 3000.00, NOW(), NOW())
        ON CONFLICT (user_id) 
        DO UPDATE SET 
            total_amount = EXCLUDED.total_amount,
            transaction_count = EXCLUDED.transaction_count,
            message = EXCLUDED.message,
            detected_at = EXCLUDED.detected_at,
            updated_at = NOW()
        """,
        Types.ROW([
            Types.STRING(),   # user_id
            Types.DOUBLE(),   # total_amount
            Types.INT(),      # transaction_count
            Types.DOUBLE(),   # total_amount (for message)
            Types.LONG()      # window_end_ms (for detected_at)
        ]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(POSTGRES_URL)
            .with_driver_name("org.postgresql.Driver")
            .with_user_name(POSTGRES_USER)
            .with_password(POSTGRES_PASSWORD)
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_size(1)              # Immediate writes for real-time
            .with_batch_interval_ms(1000)    # Max 1 second delay
            .with_max_retries(3)
            .build()
    )


# ============================================================================
# MAIN JOB
# ============================================================================
def main():
    """
    Main PyFlink DataStream job execution.
    """
    # ========================================================================
    # 1. ENVIRONMENT SETUP
    # ========================================================================
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # Enable RocksDB state backend for large state
    # env.set_state_backend(...)  # Configure RocksDB if needed
    
    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(60000)  # 60 seconds
    
    print("Environment configured")
    
    # ========================================================================
    # 2. KAFKA SOURCE
    # ========================================================================
    kafka_source = create_kafka_source()
    
    kafka_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),  # Will add watermarks after parsing
        "Debezium Transactions Source"
    )
    
    print("Kafka source created")
    
    # ========================================================================
    # 3. PARSE JSON → (user_id, amount, event_time_ms)
    # ========================================================================
    parsed_stream = kafka_stream \
        .map(
            parse_json_message,
            output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])
        ) \
        .filter(lambda x: x is not None) \
        .name("Parse JSON")
    
    print("JSON parser configured")
    
    # ========================================================================
    # 4. ASSIGN WATERMARKS
    # ========================================================================
    watermarked_stream = parsed_stream.assign_timestamps_and_watermarks(
        WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(TransactionTimestampAssigner())
    )
    
    print("Watermarks assigned")
    
    # ========================================================================
    # 5. KEY BY user_id AND APPLY BUCKETED ROLLING SUM
    # ========================================================================
    alerts_stream = watermarked_stream \
        .key_by(lambda x: x[0]) \
        .process(
            BucketedRolling30DaySum(),
            output_type=Types.ROW([
                Types.STRING(),   # user_id
                Types.DOUBLE(),   # total_amount
                Types.INT(),      # transaction_count
                Types.DOUBLE(),   # total_amount (duplicate for message)
                Types.LONG()      # window_end_ms (for detected_at)
            ])
        ) \
        .name("30-Day Bucketed Rolling Sum")
    
    print("Bucketed rolling sum configured")
    
    # ========================================================================
    # 6. JDBC SINK TO POSTGRESQL
    # ========================================================================
    jdbc_sink = create_jdbc_sink()
    alerts_stream.add_sink(jdbc_sink).name("PostgreSQL Alerts Sink")
    
    print("JDBC sink configured")
    
    # ========================================================================
    # 7. EXECUTE JOB
    # ========================================================================
    print("\n" + "="*70)
    print("Starting PyFlink Job: 30-Day Rolling Sum with Bucketed State")
    print("="*70)
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Consumer Group: {KAFKA_GROUP_ID}")
    print(f"Window Size: {WINDOW_SIZE_DAYS} days")
    print(f"Bucket Size: {BUCKET_SIZE_DAYS} day(s)")
    print(f"Threshold: ${THRESHOLD_AMOUNT}")
    print(f"PostgreSQL: {POSTGRES_URL}")
    print("="*70 + "\n")
    
    env.execute("Rolling 30-Day High-Value User Detection (Bucketed)")


if __name__ == "__main__":
    main()
