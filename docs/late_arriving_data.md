# dbt Incremental Model Demo Script

## Overview
This demo showcases how incremental models handle late-arriving data with a 7-day lookback window.

**Models:**
- `stg_order_reviews` (staging, incremental)
- `fact_reviews` (mart, incremental)

**Key Feature:** 7-day lookback window to capture late arrivals and updates

---

## Pre-Demo Setup

### 1. Connect to Snowflake
```bash
# From VS Code terminal
snowsql -a <your_account> -u <your_user> -d RAW_DATA -s PUBLIC
```

### 2. Check Current State
```sql
-- Check raw data count
SELECT COUNT(*) as total_reviews,
       MIN(REVIEW_CREATION_DATE) as oldest_review,
       MAX(REVIEW_CREATION_DATE) as newest_review
FROM raw_data.OLIST_ORDER_REVIEWS_DATASET;

-- Expected: ~99,224 reviews from 2016-01-01 to 2018-10-17
```

### 3. Initial dbt Run (Full Refresh)
```bash
cd dwh/snowflake

# Run with full refresh to establish baseline
dbt run --select stg_order_reviews fact_reviews --full-refresh

# Check results
dbt show --select stg_order_reviews --limit 5
```

### 4. Verify Initial Load
```sql
-- In Snowflake
SELECT COUNT(*) as staging_count,
       MAX(REVIEW_CREATION_DATE) as max_date,
       MAX(_LOADED_AT) as last_load_time
FROM ANALYTICS.STG_ORDER_REVIEWS;

SELECT COUNT(*) as mart_count
FROM ANALYTICS.FACT_REVIEWS;

-- Both should have same count (~99,224)
```

---

## Demo Scenario 1: New Reviews Arrive

### Setup: Insert New Reviews (Simulating New Data)
```sql
-- In Snowflake (raw_data schema)
USE DATABASE RAW_DATA;

-- Insert 3 new reviews with today's date
INSERT INTO raw_data.OLIST_ORDER_REVIEWS_DATASET (
    REVIEW_ID,
    ORDER_ID,
    REVIEW_SCORE,
    REVIEW_COMMENT_TITLE,
    REVIEW_COMMENT_MESSAGE,
    REVIEW_CREATION_DATE,
    REVIEW_ANSWER_TIMESTAMP,
    INGESTION_DATE
)
VALUES
     
    -- New review 1: Positive
    ('demo_review_001', 'demo_order_001', 5, 'Excelente!', 
     'Produto chegou rápido e bem embalado', 
     CURRENT_DATE(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
    
    -- New review 2: Negative
    ('demo_review_002', 'demo_order_002', 1, 'Péssimo', 
     'Produto com defeito e atendimento ruim', 
     CURRENT_DATE(), NULL, CURRENT_TIMESTAMP()),
    
    -- New review 3: Neutral
    ('demo_review_003', 'demo_order_003', 3, 'OK', 
     'Produto normal, nada de especial', 
     CURRENT_DATE(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Verify insertion
SELECT * FROM raw_data.OLIST_ORDER_REVIEWS_DATASET 
WHERE REVIEW_ID LIKE 'demo_review_%'
ORDER BY REVIEW_CREATION_DATE DESC;
```

### Live Demo: Run Incremental dbt
```bash
# Run incremental models
dbt run --select stg_order_reviews fact_reviews

# Expected output:
# ✅ stg_order_reviews: "Completed successfully (3 rows inserted)"
# ✅ fact_reviews: "Completed successfully (3 rows inserted)"
```

### Verify Results
```sql
-- Check staging layer
SELECT 
    REVIEW_ID,
    REVIEW_SCORE,
    REVIEW_SENTIMENT,
    REVIEW_CREATION_DATE,
    _LOADED_AT
FROM ANALYTICS.STG_ORDER_REVIEWS
WHERE REVIEW_ID LIKE 'demo_review_%'
ORDER BY REVIEW_CREATION_DATE DESC;

-- Expected: 3 new rows with today's date and review_sentiment calculated
-- demo_review_001: score=5, sentiment='positive'
-- demo_review_002: score=1, sentiment='negative'
-- demo_review_003: score=3, sentiment='neutral'

-- Check mart layer
SELECT COUNT(*) FROM ANALYTICS.FACT_REVIEWS;
-- Should increase by 3 (e.g., 99,224 → 99,227)
```

**🎤 Talking Point:**
> "Notice how dbt only processed 3 new rows instead of scanning all 99,000+ reviews. This is the power of incremental materialization with proper filtering."

---

## Demo Scenario 2: Late-Arriving Data (7 Days Ago)

### Setup: Insert Backdated Review
```sql
-- Insert a review dated 5 days ago (within 7-day lookback window)
INSERT INTO raw_data.OLIST_ORDER_REVIEWS_DATASET (
    REVIEW_ID,
    ORDER_ID,
    REVIEW_SCORE,
    REVIEW_COMMENT_TITLE,
    REVIEW_COMMENT_MESSAGE,
    REVIEW_CREATION_DATE,
    REVIEW_ANSWER_TIMESTAMP,
    INGESTION_DATE
)
VALUES
    ('demo_review_late_001', 'demo_order_late_001', 4, 'Bom produto',
     'Chegou com atraso mas produto é bom',
     DATEADD(day, -5, CURRENT_DATE()),  -- 5 days ago
     CURRENT_TIMESTAMP(), 
     CURRENT_TIMESTAMP());  -- Ingested today

-- Verify
SELECT 
    REVIEW_ID,
    REVIEW_CREATION_DATE,
    INGESTION_DATE,
    DATEDIFF(day, REVIEW_CREATION_DATE, INGESTION_DATE) as days_late
FROM raw_data.OLIST_ORDER_REVIEWS_DATASET
WHERE REVIEW_ID = 'demo_review_late_001';

-- Expected: days_late = 5
```

### Live Demo: Incremental Run Captures Late Data
```bash
# Run incremental (without full refresh)
dbt run --select stg_order_reviews fact_reviews

