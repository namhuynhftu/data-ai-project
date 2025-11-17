-- Flink SQL Job: Monitor High-Value User Transactions
-- Aggregates transactions over 300-day window and alerts on >= $3000

-- Source: Kafka CDC transactions (Avro format)
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
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`after`.`timestamp` / 1000000)),
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'postgres.streaming.transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-sql-monitor-v2',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- Sink: PostgreSQL user_alerts table
CREATE TABLE user_alerts_sink (
    user_id STRING,
    total_amount DECIMAL(12,2),
    transaction_count BIGINT,
    alert_type STRING,
    message STRING,
    severity STRING,
    detected_at TIMESTAMP(3),
    window_days INT,
    threshold_amount DECIMAL(12,2),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres_streaming:5432/streaming_db',
    'table-name' = 'streaming.user_alerts',
    'username' = 'user',
    'password' = 'password',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- Insert job: Aggregate and filter high-value users
-- Using 1-hour tumbling windows for periodic emission
INSERT INTO user_alerts_sink
SELECT
    `after`.user_id as user_id,
    SUM(ABS(`after`.amount)) as total_amount,
    COUNT(*) as transaction_count,
    'HIGH_VALUE_USER' as alert_type,
    CONCAT('User ', CAST(`after`.user_id AS STRING), ' spent $',
           CAST(SUM(ABS(`after`.amount)) AS STRING), ' total') as message,
    'HIGH' as severity,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as detected_at,
    300 as window_days,
    CAST(3000.00 AS DECIMAL(12,2)) as threshold_amount
FROM transactions_source
WHERE
    `after` IS NOT NULL
GROUP BY
    `after`.user_id,
    TUMBLE(proc_time, INTERVAL '1' MINUTE)
HAVING
    SUM(ABS(`after`.amount)) >= 3000;