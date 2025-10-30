# Brazilian E-Commerce Data Warehouse - Entity Relationship Diagram

## Why Dimensional Modeling?

We chose **Kimball dimensional modeling** (star schema) for this data warehouse because:

1. **Query Performance**: Denormalized structure reduces complex joins, enabling fast analytical queries for dashboards and reports
2. **Business User Friendly**: Dimensions represent business entities (customers, products, sellers) that business users understand intuitively
3. **Scalability**: Conformed dimensions and incremental fact loading support growing data volumes efficiently
4. **Flexibility**: Supports slice-and-dice analysis across multiple business dimensions (time, geography, product categories)
5. **Historical Tracking**: Hybrid SCD approach - Type 1 for current state (fast), Type 2 snapshots for historical analysis (accurate)

## Dimensional Model - Star Schema

```mermaid
erDiagram
    %% ==========================================
    %% DIMENSION TABLES
    %% ==========================================
    
    dim_customer {
        string customer_key PK
        string customer_id UK
        string customer_unique_id
        string customer_zip_code_prefix
        string customer_city
        string customer_state
        string geo_city
        string geo_state
    }

    dim_seller {
        string seller_key PK
        string seller_id UK
        string seller_zip_code_prefix
        string seller_city
        string seller_state
        string geo_city
        string geo_state
    }

    dim_product {
        string product_key PK
        string product_id UK
        string product_category_name
        string product_category_name_english
        int product_name_length
        int product_description_length
        int product_photos_qty
        float product_weight_g
        float product_volume_cm3
    }

    dim_date {
        date date_key PK
        int year_number
        int quarter_number
        int month_number
        int day_of_month
        int day_of_week
        boolean is_weekend
        string month_name
        string day_name
    }

    dim_payment_type {
        string payment_type_key PK
        string payment_type UK
    }

    %% ==========================================
    %% CENTRAL FACT TABLE (Most Granular)
    %% ==========================================

    fact_order_items {
        string order_item_key PK
        string order_id
        int order_item_id
        string customer_key FK
        string seller_key FK
        string product_key FK
        date purchase_date FK
        date delivered_date
        decimal price
        decimal freight_value
        decimal total_item_value
    }

    %% ==========================================
    %% SUPPORTING FACT TABLES (Less Granular)
    %% ==========================================

    fact_orders_accumulating {
        string order_id PK
        string order_status
        date purchase_date
        date approved_date
        date carrier_date
        date delivered_date
        date estimated_date
        int purchase_to_approve_days
        int approve_to_ship_days
        int ship_to_deliver_days
        int on_time_flag
    }

    fact_payments {
        string payment_key PK
        string order_id
        int payment_sequential
        string payment_type_key FK
        decimal payment_value
        int payment_installments
        decimal installment_value
    }

    fact_reviews {
        string review_key PK
        string order_id
        int review_score
        string review_sentiment
        date review_creation_date
        timestamp review_answer_timestamp
        boolean has_comment
        int comment_length
    }

    %% ==========================================
    %% METRICS MARTS
    %% ==========================================

    mart_sales_metrics {
        date order_month PK
        string customer_state PK
        string seller_state PK
        string product_category_name PK
        int total_orders
        int unique_customers
        int unique_sellers
        int total_items_sold
        decimal gross_merchandise_value
        decimal total_revenue
        decimal avg_order_value
        decimal on_time_delivery_rate
        decimal avg_review_score
    }

    mart_customer_metrics {
        string customer_key PK
        string customer_id UK
        string customer_state
        date first_order_date
        date last_order_date
        int total_orders
        decimal customer_lifetime_value
        decimal avg_order_value
        string customer_segment
        int recency_score
        int frequency_score
        int monetary_score
    }

    mart_product_metrics {
        string product_key PK
        string product_id UK
        string product_category_name
        int total_units_sold
        decimal total_sales_revenue
        decimal avg_selling_price
        decimal avg_product_rating
        string sales_tier
        string quality_tier
    }

    %% ==========================================
    %% STAR SCHEMA RELATIONSHIPS
    %% ==========================================

    %% Central Star: fact_order_items (most granular - transaction level)
    dim_customer ||--o{ fact_order_items : customer_key
    dim_seller ||--o{ fact_order_items : seller_key
    dim_product ||--o{ fact_order_items : product_key
    dim_date ||--o{ fact_order_items : purchase_date
    
    %% Supporting facts point to central fact (less granular - order level)
    fact_order_items ||--o{ fact_orders_accumulating : order_id
    fact_order_items ||--o{ fact_payments : order_id
    fact_order_items ||--o{ fact_reviews : order_id
    
    %% Payment type dimension
    dim_payment_type ||--o{ fact_payments : payment_type_key
    
    %% Metrics aggregated from central fact only
    fact_order_items }o--|| mart_sales_metrics : "aggregated_by_month_geo_category"
    fact_order_items }o--|| mart_customer_metrics : "aggregated_by_customer"
    fact_order_items }o--|| mart_product_metrics : "aggregated_by_product"
```

