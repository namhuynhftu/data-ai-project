CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- Customers table
CREATE TABLE IF NOT EXISTS olist_customers_dataset (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NULL,
    customer_zip_code_prefix VARCHAR(10) NULL,
    customer_city VARCHAR(50) NULL,
    customer_state VARCHAR(2) NULL
);

-- Geolocation table
CREATE TABLE IF NOT EXISTS olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(10) NULL,
    geolocation_lat DECIMAL(10,8) NULL,
    geolocation_lng DECIMAL(11,8) NULL,
    geolocation_city VARCHAR(50) NULL,
    geolocation_state VARCHAR(2) NULL
);

-- Order Items table
CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50) NULL,
    seller_id VARCHAR(50) NULL,
    shipping_limit_date DATETIME NULL,
    price DECIMAL(10,2) NULL,
    freight_value DECIMAL(10,2) NULL,
    PRIMARY KEY (order_id, order_item_id)
);

-- Order Payments table
CREATE TABLE IF NOT EXISTS olist_order_payments_dataset (
    order_id VARCHAR(50),
    payment_sequential INT ,
    payment_type VARCHAR(20) NULL,
    payment_installments INT NULL,
    payment_value DECIMAL(10,2) NULL,
    PRIMARY KEY (order_id, payment_sequential)
);

-- Order Reviews table
CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
    review_id VARCHAR(50) NULL,
    order_id VARCHAR(50) NULL,
    review_score INT NULL,
    review_comment_title VARCHAR(100) NULL,
    review_comment_message TEXT NULL,
    review_creation_date VARCHAR(100) NULL,
    review_answer_timestamp VARCHAR(100) NULL
);

-- Orders table
CREATE TABLE IF NOT EXISTS olist_orders_dataset (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NULL,
    order_status VARCHAR(20) NULL,
    order_purchase_timestamp DATETIME NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL
);

-- Products table
CREATE TABLE IF NOT EXISTS olist_products_dataset (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100) NULL,
    product_name_length INT NULL,
    product_description_length INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL
);

-- Sellers table
CREATE TABLE IF NOT EXISTS olist_sellers_dataset (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10) NULL,
    seller_city VARCHAR(50) NULL,
    seller_state VARCHAR(2) NULL
);

-- Product Category Name Translation table
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100) NULL
);

-- NOTE: Foreign key constraints will be added in 3_add_constraints.sql after data loading