import snowflake.connector
from dotenv import load_dotenv
import os

def verify_snowflake_setup():
    """Verify all required Snowflake objects exist."""
    load_dotenv()

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="RAW_DATA",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    try:
        cursor = conn.cursor()

        # Check warehouse
        cursor.execute("SHOW WAREHOUSES LIKE 'WH_T25'")
        if not cursor.fetchall():
            print("❌ Warehouse WH_T25 not found")
            return False

        # Check database
        cursor.execute("SHOW DATABASES LIKE 'DB_T25'")
        if not cursor.fetchall():
            print("❌ Database DB_T25 not found")
            return False

        # Check schemas
        cursor.execute("SHOW SCHEMAS IN DATABASE DB_T25")
        schemas = [row[1] for row in cursor.fetchall()]
        required_schemas = ['RAW_DATA', 'STAGING', 'ANALYTICS']

        for schema in required_schemas:
            if schema not in schemas:
                print(f"❌ Schema {schema} not found")
                return False

        # Switch to RAW_DATA schema
        cursor.execute("USE SCHEMA RAW_DATA")

        # Check file formats
        cursor.execute("SHOW FILE FORMATS")
        file_formats = [row[1] for row in cursor.fetchall()]
        if 'PARQUET_FORMAT' not in file_formats:
            print("❌ File format PARQUET_FORMAT not found")
            return False

        # Check tables
        cursor.execute("SHOW TABLES")
        tables = [row[1] for row in cursor.fetchall()]
        required_tables = [
            'OLIST_CUSTOMERS_DATASET',
            'OLIST_GEOLOCATION_DATASET', 
            'OLIST_ORDER_ITEMS_DATASET',
            'OLIST_ORDER_PAYMENTS_DATASET',
            'OLIST_ORDER_REVIEWS_DATASET',
            'OLIST_ORDERS_DATASET',
            'OLIST_PRODUCTS_DATASET',
            'OLIST_SELLERS_DATASET',
            'PRODUCT_CATEGORY_NAME_TRANSLATION'
        ]

        missing_tables = []
        for table in required_tables:
            if table not in tables:
                missing_tables.append(table)

        if missing_tables:
            print(f"❌ Missing tables: {missing_tables}")
            return False

        print("✅ All Snowflake objects verified!")
        print(f"✅ Found {len(tables)} tables")
        print(f"✅ Found file format: PARQUET_FORMAT")
        return True

    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    verify_snowflake_setup()