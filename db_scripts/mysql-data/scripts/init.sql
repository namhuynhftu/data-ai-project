-- Create database if not exists
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- 02. Customers table
CREATE TABLE IF NOT EXISTS olist_customers_dataset (
    customer_id varchar(32),
    customer_unique_id varchar(32),
    customer_zip_code_prefix int4,
    customer_city varchar(100),
    customer_state varchar(2),
    PRIMARY KEY (customer_id)
);

-- 03. Sellers table
CREATE TABLE IF NOT EXISTS olist_sellers_dataset (
    seller_id varchar(32),
    seller_zip_code_prefix int4,
    seller_city nvarchar(1000),
    seller_state varchar(2),
    PRIMARY KEY (seller_id)
);

-- 04. Product category translation
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name varchar(64),
    product_category_name_english varchar(64),
    PRIMARY KEY (product_category_name)
);

-- 05. Products table
CREATE TABLE IF NOT EXISTS olist_products_dataset (
    product_id varchar(32),
    product_category_name varchar(64),
    product_name_lenght int4,
    product_description_lenght int4,
    product_photos_qty int4,
    product_weight_g int4,
    product_length_cm int4,
    product_height_cm int4,
    product_width_cm int4,
    PRIMARY KEY (product_id)
);

-- 06. Orders table
CREATE TABLE IF NOT EXISTS olist_orders_dataset (
    order_id varchar(32),
    customer_id varchar(32),
    order_status varchar(16),
    order_purchase_timestamp varchar(32),
    order_approved_at varchar(32),
    order_delivered_carrier_date varchar(32),
    order_delivered_customer_date varchar(32),
    order_estimated_delivery_date varchar(32),
    PRIMARY KEY (order_id)
);

-- 07. Order payments
CREATE TABLE IF NOT EXISTS olist_order_payments_dataset (
    order_id varchar(32),
    payment_sequential int4,
    payment_type varchar(16),
    payment_installments int4,
    payment_value float4,
    PRIMARY KEY (order_id, payment_sequential)
);

-- 08. Order items
CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
    order_id varchar(32),
    order_item_id int4,
    product_id varchar(32),
    seller_id varchar(32),
    shipping_limit_date varchar(32),
    price float4,
    freight_value float4,
    PRIMARY KEY (order_id, order_item_id, product_id, seller_id)
);

-- 09. Order reviews
CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
    review_id varchar(32),
    order_id varchar(32),
    review_score int4,
    review_comment_title varchar(32),
    review_comment_message varchar(256),
    review_creation_date varchar(32),
    review_answer_timestamp varchar(32),
    PRIMARY KEY (review_id, order_id)
);