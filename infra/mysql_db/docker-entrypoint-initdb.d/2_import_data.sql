USE ecommerce_db;

-- Import customers data
LOAD DATA INFILE '/var/lib/data/olist_customers_dataset.csv'
INTO TABLE olist_customers_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Import sellers data
LOAD DATA INFILE '/var/lib/data/olist_sellers_dataset.csv'
INTO TABLE olist_sellers_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Import geolocation data
LOAD DATA INFILE '/var/lib/data/olist_geolocation_dataset.csv'
INTO TABLE olist_geolocation_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Import product categories
LOAD DATA INFILE '/var/lib/data/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Import products data
LOAD DATA INFILE '/var/lib/data/olist_products_dataset.csv'
INTO TABLE olist_products_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, product_category_name, @product_name_length, @product_description_length, 
 @product_photos_qty, @product_weight_g, @product_length_cm, @product_height_cm, @product_width_cm)
SET
    product_name_length = NULLIF(@product_name_length,''),
    product_description_length = NULLIF(@product_description_length,''),
    product_photos_qty = NULLIF(@product_photos_qty,''),
    product_weight_g = NULLIF(@product_weight_g,''),
    product_length_cm = NULLIF(@product_length_cm,''),
    product_height_cm = NULLIF(@product_height_cm,''),
    product_width_cm = NULLIF(@product_width_cm,'');;

-- Import orders data
LOAD DATA INFILE '/var/lib/data/olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_status, 
 @order_purchase_timestamp, @order_approved_at, 
 @order_delivered_carrier_date, @order_delivered_customer_date,
 @order_estimated_delivery_date)
SET
    order_purchase_timestamp = NULLIF(@order_purchase_timestamp,''),
    order_approved_at = NULLIF(@order_approved_at,''),
    order_delivered_carrier_date = NULLIF(@order_delivered_carrier_date,''),
    order_delivered_customer_date = NULLIF(@order_delivered_customer_date,''),
    order_estimated_delivery_date = NULLIF(@order_estimated_delivery_date,'');;

-- Import order payments
LOAD DATA INFILE '/var/lib/data/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Import order items
LOAD DATA INFILE '/var/lib/data/olist_order_items_dataset.csv'
INTO TABLE olist_order_items_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Import order reviews
LOAD DATA INFILE '/var/lib/data/olist_order_reviews_dataset.csv'
INTO TABLE olist_order_reviews_dataset
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@review_id, @order_id, @review_score, @review_comment_title, @review_comment_message, 
 @review_creation_date, @review_answer_timestamp)
SET
    review_id = @review_id,
    order_id = @order_id,
    review_score = NULLIF(@review_score, ''),
    review_comment_title = NULLIF(@review_comment_title, ''),
    review_comment_message = REPLACE(@review_comment_message, '\n', ' '),
    review_creation_date = NULLIF(@review_creation_date, ''),
    review_answer_timestamp = NULLIF(@review_answer_timestamp, '');
;

