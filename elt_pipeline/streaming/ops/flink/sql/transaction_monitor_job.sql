-- 1) Source table: same as you wrote, no change
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
    'properties.group.id' = 'flink-cumulative-v1',
    'scan.startup.mode' = 'earliest-offset',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- 2) Sink table: if you want the latest status per user, PK = user_id is fine
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

-- 3) REAL-TIME cumulative sum per user (emits on every transaction)
INSERT INTO user_alerts_sink
SELECT
    user_id,
    total_amount,
    transaction_count,
    'HIGH_VALUE_USER' AS alert_type,
    CONCAT('User ', user_id, ' spent $', CAST(total_amount AS STRING), ' total') AS message,
    'HIGH' AS severity,
    event_time AS detected_at,
    30 AS window_days,
    CAST(3000.00 AS DECIMAL(12,2)) AS threshold_amount,
    event_time AS created_at,
    event_time AS updated_at
FROM (
    SELECT
        user_id,
        event_time,
        SUM(amount) OVER (
            PARTITION BY user_id
            ORDER BY proc_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total_amount,
        CAST(COUNT(*) OVER (
            PARTITION BY user_id
            ORDER BY proc_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS INT) AS transaction_count
    FROM (
        SELECT
            `after`.user_id AS user_id,
            ABS(CAST(`after`.amount AS DECIMAL(12,2))) AS amount,
            event_time
        FROM transactions_source
        WHERE
            `after` IS NOT NULL
            AND `after`.user_id IS NOT NULL
            AND `after`.amount IS NOT NULL
            AND op IN ('c', 'u', 'r')
    )
)
WHERE total_amount >= 3000.00;
