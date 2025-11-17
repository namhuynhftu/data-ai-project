-- Flink SQL Job: High-Value User Transaction Monitor
-- This job reads CDC events from Kafka (Avro), aggregates transactions,
-- and writes alerts to PostgreSQL when users exceed $3000 in 300 days

-- ============================================================
-- 1. CREATE SOURCE TABLE: Kafka CDC Transactions (Avro)
-- ============================================================
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
        ts_ms BIGINT,
        snapshot STRING,
        db STRING,
        `schema` STRING,
        `table` STRING
    >,
    op STRING,
    ts_ms BIGINT,
    -- Convert microsecond timestamp to TIMESTAMP for time-based operations
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`after`.`timestamp` / 1000000)),
    -- Define watermark for event-time processing (5 second delay)
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'postgres.streaming.transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-sql-monitor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- ============================================================
-- 2. CREATE SINK TABLE: PostgreSQL Alerts
-- ============================================================
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
);

-- ============================================================
-- 3. EXECUTE QUERY: Aggregate and Filter High-Value Users
-- ============================================================
-- This INSERT query will run continuously, monitoring transactions
-- and inserting alerts when users exceed the threshold

INSERT INTO user_alerts_sink
SELECT 
    `after`.user_id as user_id,
    SUM(ABS(`after`.amount)) as total_amount,
    COUNT(*) as transaction_count,
    'HIGH_VALUE_USER' as alert_type,
    CONCAT('User ', CAST(`after`.user_id AS STRING), ' spent $', 
           CAST(SUM(ABS(`after`.amount)) AS STRING), ' in last 300 days') as message,
    'HIGH' as severity,
    CURRENT_TIMESTAMP as detected_at,
    300 as window_days,
    CAST(3000.00 AS DECIMAL(12,2)) as threshold_amount
FROM transactions_source
WHERE 
    `after` IS NOT NULL
    AND op IN ('c', 'r')  -- Only process CREATE and READ operations
    -- Use 10 months as workaround for DAY precision limit
    AND event_time >= CURRENT_TIMESTAMP - INTERVAL '10' MONTH
GROUP BY 
    `after`.user_id,
    TUMBLE(event_time, INTERVAL '1' MINUTE)  -- 1-minute tumbling window
HAVING 
    SUM(ABS(`after`.amount)) >= 3000;
