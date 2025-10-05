# create_snowflake_stages.py
import snowflake.connector
from dotenv import load_dotenv
import os

def create_snowflake_database():
    """Create Snowflake database schemas, tables, and file formats for the Brazilian e-commerce data."""
    load_dotenv()

    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        
        cursor = conn.cursor()
        
        print("🏗️ Creating Snowflake database structure...")
        
        # Create schemas if they don't exist
        schemas_to_create = ['RAW_DATA', 'STAGING', 'ANALYTICS']
        
        for schema in schemas_to_create:
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                print(f"✅ Schema {schema} created/verified")
            except Exception as e:
                print(f"⚠️ Schema {schema}: {e}")
        
        # Switch to RAW_DATA schema for creating objects
        cursor.execute("USE SCHEMA RAW_DATA")
        
        print(f"📋 Working in schema: RAW_DATA")
        
        # Create file formats that might be useful for future data loading
        try:
            cursor.execute("""
            CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
            TYPE = 'PARQUET'
            COMPRESSION = 'SNAPPY'
            """)
            print(f"✅ File format PARQUET_FORMAT created successfully")
        except Exception as e:
            print(f"❌ Error creating file format: {e}")

        # Create table structures for the Brazilian e-commerce data
        print("\n🏗️ Creating table structures...")
        
        tables_sql = {
            "olist_customers": """
            CREATE OR REPLACE TABLE olist_customers_dataset (
                customer_id VARCHAR(50) PRIMARY KEY,
                customer_unique_id VARCHAR(50),
                customer_zip_code_prefix VARCHAR(10),
                customer_city VARCHAR(50),
                customer_state VARCHAR(2)
            )
            """,
            "olist_geolocation": """
            CREATE OR REPLACE TABLE olist_geolocation_dataset (
                geolocation_zip_code_prefix VARCHAR(10),
                geolocation_lat DECIMAL(10,8),
                geolocation_lng DECIMAL(11,8),
                geolocation_city VARCHAR(50),
                geolocation_state VARCHAR(2)
            )
            """,
            "olist_order_items": """
            CREATE OR REPLACE TABLE olist_order_items_dataset (
                order_id VARCHAR(50),
                order_item_id INT,
                product_id VARCHAR(50),
                seller_id VARCHAR(50),
                shipping_limit_date TIMESTAMP,
                price DECIMAL(10,2),
                freight_value DECIMAL(10,2),
                PRIMARY KEY (order_id, order_item_id)
            )
            """,
            "olist_order_payments": """
            CREATE OR REPLACE TABLE olist_order_payments_dataset (
                order_id VARCHAR(50),
                payment_sequential INT,
                payment_type VARCHAR(20),
                payment_installments INT,
                payment_value DECIMAL(10,2),
                PRIMARY KEY (order_id, payment_sequential)
            )
            """,
            "olist_order_reviews": """
            CREATE OR REPLACE TABLE olist_order_reviews_dataset (
                review_id VARCHAR(50),
                order_id VARCHAR(50),
                review_score INT,
                review_comment_title VARCHAR(100),
                review_comment_message TEXT,
                review_creation_date VARCHAR(100),
                review_answer_timestamp VARCHAR(100)
            )
            """,
            "olist_orders": """
            CREATE OR REPLACE TABLE olist_orders_dataset (
                order_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                order_status VARCHAR(20),
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
            )
            """,
            "olist_products": """
            CREATE OR REPLACE TABLE olist_products_dataset (
                product_id VARCHAR(50) PRIMARY KEY,
                product_category_name VARCHAR(100),
                product_name_length INT,
                product_description_length INT,
                product_photos_qty INT,
                product_weight_g INT,
                product_length_cm INT,
                product_height_cm INT,
                product_width_cm INT
            )
            """,
            "olist_sellers": """
            CREATE OR REPLACE TABLE olist_sellers_dataset (
                seller_id VARCHAR(50) PRIMARY KEY,
                seller_zip_code_prefix VARCHAR(10),
                seller_city VARCHAR(50),
                seller_state VARCHAR(2)
            )
            """,
            "product_category_translation": """
            CREATE OR REPLACE TABLE product_category_name_translation (
                product_category_name VARCHAR(100) PRIMARY KEY,
                product_category_name_english VARCHAR(100)
            )
            """
        }
        
        for table_name, sql in tables_sql.items():
            try:
                cursor.execute(sql)
                print(f"✅ Table {table_name} created successfully")
            except Exception as e:
                print(f"❌ Error creating table {table_name}: {e}")

        # Add foreign key constraints (optional in Snowflake - mainly for documentation)
        print("\n🔗 Adding foreign key constraints...")
        
        foreign_key_constraints = [
            # Order Items foreign keys
            """
            ALTER TABLE olist_order_items_dataset
            ADD CONSTRAINT fk_order_items_orders 
            FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id)
            """,
            """
            ALTER TABLE olist_order_items_dataset
            ADD CONSTRAINT fk_order_items_products 
            FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id)
            """,
            """
            ALTER TABLE olist_order_items_dataset
            ADD CONSTRAINT fk_order_items_sellers 
            FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset(seller_id)
            """,
            # Order Payments foreign key
            """
            ALTER TABLE olist_order_payments_dataset
            ADD CONSTRAINT fk_order_payments_orders 
            FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id)
            """,
            # Order Reviews foreign key
            """
            ALTER TABLE olist_order_reviews_dataset
            ADD CONSTRAINT fk_order_reviews_orders 
            FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id)
            """,
            # Orders foreign key
            """
            ALTER TABLE olist_orders_dataset
            ADD CONSTRAINT fk_orders_customers 
            FOREIGN KEY (customer_id) REFERENCES olist_customers_dataset(customer_id)
            """
        ]
        
        for i, constraint_sql in enumerate(foreign_key_constraints, 1):
            try:
                cursor.execute(constraint_sql)
                print(f"✅ Foreign key constraint {i} added successfully")
            except Exception as e:
                print(f"⚠️ Foreign key constraint {i}: {e} (Note: FK constraints are informational in Snowflake)")

        print("\n✅ Snowflake database setup completed!")
        print("📊 Summary:")
        print("   - 3 schemas created (RAW_DATA, STAGING, ANALYTICS)")
        print("   - 1 file format created (PARQUET_FORMAT)")
        print("   - 9 tables created with proper schemas")
        print("   - 6 foreign key constraints added")
        print("\n💡 Next step: Use load_data_to_snowflake.py to load data directly from your ELT pipeline")
        
        
    except Exception as e:
        print(f"❌ Failed to create stages: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_snowflake_database()