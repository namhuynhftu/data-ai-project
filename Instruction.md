# 🎬 Capstone Presentation Commands Cheat Sheet

## Quick Reference for Live Demo (20 minutes)

---

## 🚀 Pre-Demo Setup (Run before presentation)

### 1. Start Docker Services
```bash
# Navigate to project directory
cd /d/2_learning/fa-dae2-capstone-namhuynh

# Start all Docker containers
docker-compose up -d

# Verify all services are running
docker-compose ps
```

### 2. Activate Python Environment
```bash
# Activate virtual environment
source .venv/Scripts/activate  # Windows Git Bash
# OR
.venv\Scripts\activate.bat     # Windows CMD

# Verify environment
python --version
pip list | grep snowflake
```

### 3. Test Connections (Optional - if time permits)
```bash
# Test all connections quickly
python test_mysql_connection.py
python test_postgresql_connection.py  
python test_snowflake_setup.py
python test_minio_connection.py
```

---

## 📊 Demo Section 1: Data Ingestion & Pipeline Foundation (8 minutes)

### A. Show Repository Structure & Git Workflow
```bash
# Show branches and commit history
git branch -a
git log --oneline -10

# Show meaningful commits
git log --oneline --grep="dev:" -5
```

### B. Demonstrate Batch Pipeline (MySQL → MinIO → Snowflake)
```bash
# Run the complete batch pipeline
python elt_pipeline/batch/pipelines/main.py 
# Select option for full load and incremental load


```

### C. Demonstrate Streaming Pipeline (Real-time → PostgreSQL)
```bash
# Run streaming pipeline demo
python elt_pipeline/streaming/pipelines/main.py 

# Every run check data on DBeaver to see that data arrive
select 
    count(1) as num_of_txns, 
	(select count(1) from detailed_transactions) as num_of_detailed_txns, 
	(select count(1) from users) as total_users
from transactions
```

### D. Show Live Data Collection
```sql
-- Show data has been collected in DBeaver

select count(1) as num_of_txns, 
	(select count(1) from detailed_transactions) as num_of_detailed_txns, 
	(select count(1) from users) as total_users
from transactions
-- Show MinIO data
-- Open browser: http://localhost:9000 (username: minioadmin, password: minioadmin)

-- # Show Snowflake data (if connected)
-- #1. Full Tables Check
SELECT 
	count(1) AS customers, 
	(SELECT count(1) FROM OLIST_GEOLOCATION_DATASET) AS geolocation_records, 
	(SELECT count(1) FROM OLIST_ORDER_ITEMS_DATASET) AS order_items_records, 
	(SELECT count(1) FROM OLIST_ORDER_PAYMENTS_DATASET) AS order_payments_records, 
	(SELECT count(1) FROM OLIST_ORDER_REVIEWS_DATASET) AS order_reviews_records, 
	(SELECT count(1) FROM OLIST_ORDERS_DATASET) AS orders_records, 
	(SELECT count(1) FROM OLIST_PRODUCTS_DATASET) AS product_records, 
	(SELECT count(1) FROM OLIST_SELLERS_DATASET) AS seller_records, 
	(SELECT count(1) FROM PRODUCT_CATEGORY_NAME_TRANSLATION ) AS product_cat_records
FROM olist_customers_dataset; 
-- #2. Incremental Load Tables Check

SELECT
	count(1) AS orders_records, 
	(SELECT count(1) FROM OLIST_ORDER_REVIEWS_DATASET) AS order_reviews_records,
	(SELECT count(1) FROM OLIST_ORDER_ITEMS_DATASET) AS order_items_records
FROM OLIST_ORDERS_DATASET
-- #3.Full Load Tables Check
SELECT 
	count(1) AS customers, 
	(SELECT count(1) FROM OLIST_GEOLOCATION_DATASET) AS geolocation_records, 
	(SELECT count(1) FROM OLIST_ORDER_PAYMENTS_DATASET) AS order_payments_records, 
	(SELECT count(1) FROM OLIST_PRODUCTS_DATASET) AS product_records, 
	(SELECT count(1) FROM OLIST_SELLERS_DATASET) AS seller_records, 
	(SELECT count(1) FROM PRODUCT_CATEGORY_NAME_TRANSLATION ) AS product_cat_records
FROM olist_customers_dataset; 

```

---

## 🔄 Demo Section 2: Data Transformation & Modeling (8 minutes)

### A. Show dbt Project Structure
```bash
# Navigate to dbt project
cd dwh/snowflake


```

### B. Execute dbt Models (Live Demo)
```bash
# Run all staging models
dbt run --models staging

# Run intermediate models
dbt run --models intermediate

# Run mart models  
dbt run --models mart

# Run incremental model demo
dbt run --models stg_orders --full-refresh
dbt run --models stg_orders  # Show incremental behavior
```

### C. Show Data Quality Tests
```bash
# Run dbt tests
dbt test

# Run specific custom tests
dbt test --models assert_positive_revenue_for_delivered_orders
dbt test --models assert_valid_order_dates
```

### D. Show Custom Macros in Action
```bash
# Show macro usage in models
cat models/mart/dimensions/dim_customer.sql | grep -A5 -B5 "calculate_customer_segment"

# Compile and show macro output
dbt compile --models dim_customer
cat target/compiled/fa_dae2_capstone/models/mart/dimensions/dim_customer.sql
```

### E. Demonstrate Incremental/Snapshot Models
```bash
# Show snapshot models
ls snapshots/

# Run snapshots
dbt snapshot

# Show incremental strategy
cat models/staging/stg_orders.sql | grep -A10 "config"
```

---

## 🛠️ Demo Section 3: DevOps & CI Foundation (3 minutes)

### A. Show CI/CD Pipeline
```bash
# Show GitHub Actions workflow
cat .github/workflows/pr_ci.yml

# Show recent workflow runs in browser
# Navigate to: https://github.com/namhuynhftu/fa-dae2-capstone-namhuynh/actions
```

### B. Demonstrate CI Checks
```bash
# Show SQLFluff configuration
cat .sqlfluff

# Run SQLFluff locally (same as CI)
sqlfluff lint dwh/snowflake/models/

# Show dbt commands used in CI
dbt parse
dbt compile
```
