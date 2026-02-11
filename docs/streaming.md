# Streaming Pipeline Testing Guide

## Overview

This guide provides comprehensive test scenarios for validating the **Flink 30-Day Rolling Window** stream processing job. The job monitors user spending patterns and generates alerts when users exceed $3,000 in total spending within a rolling 30-day window.

**Architecture:**
```
PostgreSQL (streaming.transactions) 
  → Debezium CDC 
  → Kafka (postgres.streaming.transactions)
  → Flink (30-Day Rolling Window)
  → PostgreSQL (streaming.user_alerts)
```

**Key Parameters:**
- **Window Size:** 30 days (rolling)
- **Threshold:** $3,000 USD
- **Alert Type:** HIGH_VALUE_USER
- **Bucket Strategy:** Daily buckets for efficient state management

---

## Prerequisites

1. **Start Streaming Infrastructure:**
   ```bash
   docker-compose -f docker/docker-compose.streaming.yml up -d
   ```

2. **Verify Services Running:**
   ```bash
   # Check Debezium
   curl -s http://localhost:8083/connectors/postgres-cdc-connector/status | jq .connector.state
   
   # Check Flink JobManager
   curl -s http://localhost:8081/jobs | jq .
   ```

3. **Start Flink Job:**
   ```bash
   cd elt_pipeline/streaming/ops/flink
   bash submit_bucketed_job.sh
   ```

4. **Connect to PostgreSQL:**
   ```bash
   docker exec -it postgres_streaming psql -U user -d streaming_db
   ```

---

## Test Setup

### Create Test User

**Choose one of the following user_ids (pick any that doesn't already exist):**

```sql
-- Option 1: Test User 1
INSERT INTO streaming.users (user_id, name, email, created_at)
VALUES (
    'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d'::UUID,
    'Nam Huynh Test 1',
    'namhuynh.test1@example.com',
    CURRENT_TIMESTAMP
)
ON CONFLICT (user_id) DO NOTHING;

-- Option 2: Test User 2
INSERT INTO streaming.users (user_id, name, email, created_at)
VALUES (
    'f9e8d7c6-b5a4-4321-9876-543210fedcba'::UUID,
    'Nam Huynh Test 2',
    'namhuynh.test2@example.com',
    CURRENT_TIMESTAMP
)
ON CONFLICT (user_id) DO NOTHING;

-- Option 3: Test User 3
INSERT INTO streaming.users (user_id, name, email, created_at)
VALUES (
    '12345678-90ab-4cde-f012-3456789abcde'::UUID,
    'Nam Huynh Test 3',
    'namhuynh.test3@example.com',
    CURRENT_TIMESTAMP
)
ON CONFLICT (user_id) DO NOTHING;

-- Option 4: Test User 4
INSERT INTO streaming.users (user_id, name, email, created_at)
VALUES (
    'abcdef01-2345-4678-90ab-cdef01234567'::UUID,
    'Nam Huynh Test 4',
    'namhuynh.test4@example.com',
    CURRENT_TIMESTAMP
)
ON CONFLICT (user_id) DO NOTHING;

-- Option 5: Test User 5
INSERT INTO streaming.users (user_id, name, email, created_at)
VALUES (
    '11111111-2222-4333-8444-555555555555'::UUID,
    'Nam Huynh Test 5',
    'namhuynh.test5@example.com',
    CURRENT_TIMESTAMP
)
ON CONFLICT (user_id) DO NOTHING;
```

**For the rest of this guide, replace `<TEST_USER_ID>` with your chosen UUID from above.**

**Quick copy UUIDs:**
- User 1: `a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d`
- User 2: `f9e8d7c6-b5a4-4321-9876-543210fedcba`
- User 3: `12345678-90ab-4cde-f012-3456789abcde`
- User 4: `abcdef01-2345-4678-90ab-cdef01234567`
- User 5: `11111111-2222-4333-8444-555555555555`

---

## Test Scenarios

### TEST 1: Normal Transaction (BELOW Threshold)

**Objective:** Verify that transactions below the threshold do NOT trigger alerts.

**Test:**
```sql
INSERT INTO streaming.transactions (user_id, amount, currency, timestamp)
VALUES (
    '<TEST_USER_ID>'::UUID,  -- Replace with your chosen user_id
    2000.00,
    'USD',
    CURRENT_TIMESTAMP
);
```

**Expected Result:**
- ✅ Transaction appears in `streaming.transactions`
- ❌ NO alert in `streaming.user_alerts` (total: $2,000 < $3,000)

**Verification:**
```sql
-- Check transactions
SELECT transaction_id, user_id, amount, timestamp
FROM streaming.transactions
WHERE user_id = '<TEST_USER_ID>'::UUID
ORDER BY timestamp DESC;

-- Check alerts (should be empty)
SELECT * FROM streaming.user_alerts 
WHERE user_id = '<TEST_USER_ID>';
```

**Wait Time:** 5-10 seconds for CDC + Flink processing

---

### TEST 2: Old Transaction (OUTSIDE 30-Day Window)

**Objective:** Verify that transactions older than 30 days are excluded from the window.

**Test:**
```sql
INSERT INTO streaming.transactions (user_id, amount, currency, timestamp)
VALUES (
    '<TEST_USER_ID>'::UUID,  -- Replace with your chosen user_id
    2000.00,
    'USD',
    CURRENT_TIMESTAMP - INTERVAL '35 days'  -- Outside 30-day window
);
```

**Expected Result:**
- ✅ Transaction appears in `streaming.transactions`
- ❌ NO alert in `streaming.user_alerts` (old transaction excluded)
- Total within 30 days: $2,000 (from Test 1 only)

**Verification:**
```sql
SELECT 
    transaction_id,
    user_id,
    amount,
    timestamp,
    CURRENT_TIMESTAMP - timestamp AS age
FROM streaming.transactions
WHERE user_id = '<TEST_USER_ID>'::UUID
ORDER BY timestamp DESC;
```

---

### TEST 3: High-Value Transaction (EXCEEDS Threshold) 🚨

**Objective:** Verify alert generation when total spending exceeds $3,000 within 30 days.

**Test:**
```sql
INSERT INTO streaming.transactions (user_id, amount, currency, timestamp)
VALUES (
    '<TEST_USER_ID>'::UUID,  -- Replace with your chosen user_id
    2600.00,
    'USD',
    CURRENT_TIMESTAMP
);
```

**Expected Result:**
- ✅ Transaction appears in `streaming.transactions`
- ✅ **ALERT GENERATED** in `streaming.user_alerts`
- Total within 30 days: $2,000 (Test 1) + $2,600 (Test 3) = **$4,600** > $3,000 threshold

**Verification:**
```sql
-- Check all transactions
SELECT 
    transaction_id,
    amount,
    timestamp,
    CURRENT_TIMESTAMP - timestamp AS age
FROM streaming.transactions
WHERE user_id = '<TEST_USER_ID>'::UUID
ORDER BY timestamp DESC;

-- Check alert (should exist now!)
SELECT 
    user_id,
    total_amount,
    transaction_count,
    alert_type,
    message,
    severity,
    detected_at,
    created_at
FROM streaming.user_alerts 
WHERE user_id = '<TEST_USER_ID>';
```

**Expected Alert Row:**
```
user_id:            <TEST_USER_ID>
total_amount:       4600.00
transaction_count:  2
alert_type:         HIGH_VALUE_USER
message:            User spent $4600.00 in last 30 days
severity:           HIGH
window_days:        30
threshold_amount:   3000.00
```

---

## CDC Event Testing (UPDATE/DELETE)

### TEST 4: UPDATE Transaction - Reduce Amount (Amount DOWN)

**Objective:** Verify that Flink processes UPDATE CDC events correctly and recalculates the window.

**Step 1 - Find Transaction ID:**
```sql
SELECT transaction_id, amount, timestamp
FROM streaming.transactions
WHERE user_id = '<TEST_USER_ID>'::UUID
  AND amount = 2600.00
ORDER BY timestamp DESC
LIMIT 1;
```

**Step 2 - Update Amount (copy transaction_id from Step 1):**
```sql
-- Replace with actual transaction_id from Step 1
UPDATE streaming.transactions
SET amount = 500.00
WHERE transaction_id = '<TRANSACTION_ID_HERE>';
```

**Expected Result:**
- New total: $2,000 + $500 = **$2,500** < $3,000
- Alert may be updated or deleted (depends on Flink job implementation)

**Verification:**
```sql
SELECT transaction_id, amount, timestamp
FROM streaming.transactions
WHERE user_id = '9c7e4a37-62ad-4c70-a9c7-3b4a5c61a4be'::UUID
ORDER BY timestamp DESC;

SELECT * FROM streaming.user_alerts
WHERE user_id = '<TEST_USER_ID>';
```

---

### TEST 5: UPDATE Transaction - Increase Amount (Amount UP)

**Objective:** Verify that increasing transaction amounts triggers alert updates.

**Test:**
```sql
-- Use same transaction_id as TEST 4
UPDATE streaming.transactions
SET amount = 4000.00
WHERE transaction_id = '<TRANSACTION_ID_HERE>';
```

**Expected Result:**
- New total: $2,000 + $4,000 = **$6,000** > $3,000
- Alert should be updated with new total

**Verification:**
```sql
SELECT transaction_id, amount, timestamp
FROM streaming.transactions
WHERE user_id = '9c7e4a37-62ad-4c70-a9c7-3b4a5c61a4be'::UUID
ORDER BY timestamp DESC;

SELECT 
    total_amount,
    transaction_count,
    updated_at
FROM streaming.user_alerts
WHERE user_id = '<TEST_USER_ID>';
```

---

### TEST 6: DELETE Transaction (Outside Window)

**Objective:** Verify DELETE CDC events are handled correctly.

**Step 1 - Find Old Transaction:**
```sql
SELECT transaction_id, amount, timestamp
FROM streaming.transactions
WHERE user_id = '<TEST_USER_ID>'::UUID
  AND timestamp < CURRENT_TIMESTAMP - INTERVAL '30 days';
```

**Step 2 - Delete Transaction:**
```sql
DELETE FROM streaming.transactions
WHERE transaction_id = '<OLD_TRANSACTION_ID>';
```

**Expected Result:**
- No impact on 30-day window (transaction was already outside)
- Debezium emits DELETE CDC event (for testing purposes)

**Verification:**
```sql
SELECT COUNT(*) as remaining_transactions
FROM streaming.transactions
WHERE user_id = '<TEST_USER_ID>'::UUID;
```

---

### TEST 7: DELETE All User Data (Cascade)

**Objective:** Test full cleanup and cascade deletes.

**Test:**
```sql
-- Delete user (will cascade to transactions if FK configured)
DELETE FROM streaming.users
WHERE user_id = '9c7e4a37-62ad-4c70-a9c7-3b4a5c61a4be'::UUID;
```

**Alternative (if no cascade):**
```sql
-- First delete transactions
DELETE FROM streaming.transactions
WHERE user_id = '9c7e4a37-62ad-4c70-a9c7-3b4a5c61a4be'::UUID;

-- Then delete user
DELETE FROM streaming.users
WHERE user_id = '9c7e4a37-62ad-4c70-a9c7-3b4a5c61a4be'::UUID;
```

**Verification:**
```sql
-- Should return 0 rows
SELECT * FROM streaming.transactions 
WHERE user_id = '<TEST_USER_ID>';

SELECT * FROM streaming.users 
WHERE user_id = '<TEST_USER_ID>';
```

---

## Multi-User Testing

### TEST 8: User Isolation

**Objective:** Verify that alerts are correctly isolated per user (keyed by user_id).

**Setup Second User:**
```sql
INSERT INTO streaming.users (user_id, name, email, created_at)
VALUES (
    '4f2d8b3e-3d2a-41c3-88ac-fd92af0d9057'::UUID,
    'Alice Second',
    'alice001@example.com',
    CURRENT_TIMESTAMP
)
ON CONFLICT (user_id) DO NOTHING;

-- Single high-value transaction exceeding threshold
INSERT INTO streaming.transactions (user_id, amount, currency, timestamp)
VALUES (
    '4f2d8b3e-3d2a-41c3-88ac-fd92af0d9057'::UUID,
    5000.00,
    'USD',
    CURRENT_TIMESTAMP
);
```

**Expected Result:**
- Alice immediately triggers alert (total: $5,000 > $3,000)
- Nam's alert remains independent

**Verification:**
```sql
-- Check both users' alerts
SELECT 
    user_id,
    total_amount,
    transaction_count,
    alert_type,
    created_at
FROM streaming.user_alerts 
WHERE user_id IN (
    '<TEST_USER_ID>'::UUID,  -- Nam (your test user)
    '4f2d8b3e-3d2a-41c3-88ac-fd92af0d9057'::UUID   -- Alice
)
ORDER BY created_at DESC;
```

---

## Monitoring & Debugging

### Check Flink Job Status
```bash
# View all jobs
curl -s http://localhost:8081/jobs | jq .

# Get specific job details
curl -s http://localhost:8081/jobs/<JOB_ID> | jq .
```

### Check Kafka Topics
```bash
# List topics
docker exec kafka_broker kafka-topics --bootstrap-server kafka:9092 --list | grep transaction

# Consume messages (JSON topic)
docker exec kafka_broker kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic postgres.streaming.transactions.json \
  --from-beginning \
  --max-messages 5
```

### Check Debezium Connector
```bash
# Status
curl -s http://localhost:8083/connectors/postgres-cdc-connector/status | jq .

# Config
curl -s http://localhost:8083/connectors/postgres-cdc-connector | jq .
```

### View Flink Logs
```bash
# JobManager logs
docker logs flink_jobmanager -f --tail 100

# TaskManager logs
docker logs flink_taskmanager -f --tail 100
```

### Check Consumer Groups
```bash
# Flink consumer group offsets
docker exec kafka_broker kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group pyflink-bucketed-30day-v6 \
  --describe
```

---

## Expected Behavior Summary

| Test # | Action | Expected Total (30d) | Alert? | Notes |
|--------|--------|---------------------|--------|-------|
| 1 | Insert $2,000 | $2,000 | ❌ No | Below threshold |
| 2 | Insert $2,000 (35d old) | $2,000 | ❌ No | Outside window |
| 3 | Insert $2,600 | $4,600 | ✅ Yes | Exceeds $3,000 |
| 4 | Update to $500 | $2,500 | ❌ No | Back below threshold |
| 5 | Update to $4,000 | $6,000 | ✅ Yes | Exceeds again |
| 6 | Delete old tx | $6,000 | ✅ Yes | No change (was outside window) |
| 7 | Delete user | N/A | N/A | Cleanup |
| 8 | New user $5,000 | $5,000 | ✅ Yes | Independent user |

---

## Common Issues & Troubleshooting

### Issue: No Alerts Appearing

**Possible Causes:**
1. Flink job not running
2. Kafka consumer lag
3. CDC connector not capturing changes

**Debug Steps:**
```bash
# 1. Check Flink job is RUNNING
curl -s http://localhost:8081/jobs | jq '.jobs[] | select(.status=="RUNNING")'

# 2. Check Kafka has messages
docker exec kafka_broker kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group pyflink-bucketed-30day-v6 \
  --describe

# 3. Check Debezium is capturing
curl -s http://localhost:8083/connectors/postgres-cdc-connector/status
```

### Issue: Alerts Not Updating

**Possible Causes:**
- Flink job using old consumer group offsets
- State not being cleared between tests

**Solution:**
```bash
# Cancel Flink job
curl -X PATCH http://localhost:8081/jobs/<JOB_ID>?mode=cancel

# Change consumer group in rolling_30day_bucketed.py
KAFKA_GROUP_ID = "pyflink-bucketed-30day-v7"  # Increment version

# Resubmit job
cd elt_pipeline/streaming/ops/flink
bash submit_bucketed_job.sh
```

### Issue: Duplicate Alerts

**Possible Causes:**
- Multiple Flink job instances running
- Consumer group not committing offsets

**Solution:**
```bash
# List all running jobs
curl -s http://localhost:8081/jobs | jq '.jobs[] | select(.status=="RUNNING")'

# Cancel duplicates
curl -X PATCH http://localhost:8081/jobs/<DUPLICATE_JOB_ID>?mode=cancel
```

---

## Clean Up

### Reset Test Data
```sql
-- Clear alerts
DELETE FROM streaming.user_alerts 
WHERE user_id IN (
    '<TEST_USER_ID>',
    '4f2d8b3e-3d2a-41c3-88ac-fd92af0d9057'
);

-- Clear transactions
DELETE FROM streaming.transactions 
WHERE user_id IN (
    '<TEST_USER_ID>',
    '4f2d8b3e-3d2a-41c3-88ac-fd92af0d9057'
);

-- Clear users
DELETE FROM streaming.users 
WHERE user_id IN (
    '<TEST_USER_ID>',
    '4f2d8b3e-3d2a-41c3-88ac-fd92af0d9057'
);
```

### Stop Services
```bash
docker-compose -f docker/docker-compose.streaming.yml down
```

---

## Additional Test Ideas

1. **Time-Based Tests:**
   - Insert transactions at window boundaries (exactly 30 days old)
   - Test with transactions in future timestamps

2. **Currency Testing:**
   - Mix USD, EUR, GBP transactions
   - Verify currency handling in alerts

3. **High Volume Testing:**
   - Insert 1000+ transactions rapidly
   - Monitor Flink backpressure and throughput

4. **State Recovery:**
   - Restart Flink job mid-processing
   - Verify state is restored correctly

5. **Late Arrivals:**
   - Insert out-of-order transactions
   - Verify watermark handling

---

## References

- Flink Dashboard: http://localhost:8081
- Debezium Connect: http://localhost:8083
- Schema Registry: http://localhost:8081
- Kafka Topics: `postgres.streaming.transactions`, `postgres.streaming.transactions.json`

**Job File:** [`elt_pipeline/streaming/ops/flink/rolling_30day_bucketed.py`](../elt_pipeline/streaming/ops/flink/rolling_30day_bucketed.py)
