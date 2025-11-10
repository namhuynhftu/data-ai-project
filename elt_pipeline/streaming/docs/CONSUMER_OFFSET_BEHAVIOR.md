# Flink CDC Monitoring - Consumer Offset Behavior

## How `auto.offset.reset: "earliest"` Works

### First Run (No Consumer Group Exists)
- Consumer group: `flink-monitor-group-v3`
- Behavior: Reads **all messages from the beginning** of the topic
- Example: Processes messages 1-200 from `postgres.streaming.transactions`

### Subsequent Runs (Consumer Group Exists)
- Behavior: **Resumes from last committed offset**
- The consumer group tracks which messages have been processed
- **Does NOT re-read from beginning**

## Example Scenario

### Run 1: Initial Processing
```
Messages in topic: 1-200
Consumer starts at: Message 1 (earliest)
Processes: Messages 1-200
Last committed offset: 200
Alerts generated: User 27956157 ($3,441.50)
```

### Stop the Script
- Consumer group offset saved: 200
- No data lost

### Run 2: Resume Processing
```
Messages in topic: 1-250 (50 new messages arrived)
Consumer starts at: Message 201 (last committed offset)
Processes: Messages 201-250
Alerts: Only if new users exceed threshold
```

## Key Points

1. **`earliest`** only applies when consumer group has NO offset
   - First time running with a new group.id
   - Or when consumer group is deleted

2. **Offset commits happen automatically**
   - After each message is processed successfully
   - Ensures "exactly-once" processing

3. **To reprocess from beginning:**
   - Change `group.id` to a new name (e.g., v4, v5)
   - OR delete the consumer group:
     ```bash
     docker exec kafka_broker kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group flink-monitor-group-v3
     ```

## Current Configuration

```python
self.consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "flink-monitor-group-v3",  # Change this to reset
    "auto.offset.reset": "earliest",
})
```

## Alert Accuracy

The script now queries PostgreSQL directly to ensure alert totals match the actual database:

```python
# Query actual database for accurate totals
cursor.execute("""
    SELECT COUNT(*), COALESCE(SUM(ABS(amount)), 0)
    FROM streaming.transactions 
    WHERE user_id = %s 
    AND timestamp >= NOW() - INTERVAL '%s days'
""", (user_id, self.window_days))
```

This ensures the alert shows the **complete, accurate total** from the database, not just the in-memory calculation.
