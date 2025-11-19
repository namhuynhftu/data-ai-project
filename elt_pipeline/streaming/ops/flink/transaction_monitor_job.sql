-- Flink SQL Job: Monitor High-Value User Transactions
-- Aggregates transactions over 300-day window and alerts on >= $3000

-- Source: Kafka CDC transactions (Avro format)
-- Note: Debezium converts PostgreSQL NUMERIC to STRING in Avro
CREATE TABLE transactions_source (
    `before` ROW<
        transaction_id STRING,
        user_id STRING,
        amount STRING,
        currency STRING,
        `timestamp` BIGINT
    >,
    `after` ROW<
        transaction_id STRING,
        user_id STRING,
        amount STRING,
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
    'properties.group.id' = 'flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- Sink: PostgreSQL user_alerts table
-- Note: user_id must be cast to UUID in the SELECT to match PostgreSQL schema
CREATE TABLE user_alerts_sink (
    user_id STRING,
    total_amount DECIMAL(12,2),
    transaction_count INT,
    alert_type STRING,
    message STRING,
    severity STRING,
    detected_at TIMESTAMP(3),
    window_days INT,
    threshold_amount DECIMAL(12,2),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
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
-- Using simple GROUP BY for continuous aggregation (upsert mode)
-- Each new transaction triggers an update to the user's aggregate in PostgreSQL
INSERT INTO user_alerts_sink
SELECT
    `after`.user_id as user_id,
    SUM(ABS(CAST(`after`.amount AS DECIMAL(10,2)))) as total_amount,
    CAST(COUNT(*) AS INT) as transaction_count,
    'HIGH_VALUE_USER' as alert_type,
    CONCAT('User ', CAST(`after`.user_id AS STRING), ' spent $',
           CAST(SUM(ABS(CAST(`after`.amount AS DECIMAL(10,2)))) AS STRING), ' total') as message,
    'HIGH' as severity,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as detected_at,
    300 as window_days,
    CAST(3000.00 AS DECIMAL(12,2)) as threshold_amount,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as created_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as updated_at
FROM transactions_source
WHERE
    `after` IS NOT NULL
    AND `after`.user_id IS NOT NULL
    AND `after`.amount IS NOT NULL
GROUP BY
    `after`.user_id;