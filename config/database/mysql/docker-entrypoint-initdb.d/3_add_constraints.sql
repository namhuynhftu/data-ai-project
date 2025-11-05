USE ecommerce_db;

-- Add foreign key constraints after data loading
-- Only add constraints for records that actually exist

-- First, clean up orphaned records that would violate foreign key constraints
DELETE FROM olist_order_items_dataset 
WHERE order_id NOT IN (SELECT order_id FROM olist_orders_dataset);

DELETE FROM olist_order_items_dataset 
WHERE product_id NOT IN (SELECT product_id FROM olist_products_dataset);

DELETE FROM olist_order_items_dataset 
WHERE seller_id NOT IN (SELECT seller_id FROM olist_sellers_dataset);

DELETE FROM olist_order_payments_dataset 
WHERE order_id NOT IN (SELECT order_id FROM olist_orders_dataset);

DELETE FROM olist_order_reviews_dataset 
WHERE order_id NOT IN (SELECT order_id FROM olist_orders_dataset);

DELETE FROM olist_orders_dataset 
WHERE customer_id NOT IN (SELECT customer_id FROM olist_customers_dataset);

-- Now add the foreign key constraints
ALTER TABLE olist_order_items_dataset
    ADD CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id),
    ADD CONSTRAINT fk_order_items_products FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id),
    ADD CONSTRAINT fk_order_items_sellers FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset(seller_id);

ALTER TABLE olist_order_payments_dataset
    ADD CONSTRAINT fk_order_payments_orders FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id);

ALTER TABLE olist_order_reviews_dataset
    ADD CONSTRAINT fk_order_reviews_orders FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id);

ALTER TABLE olist_orders_dataset
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES olist_customers_dataset(customer_id);