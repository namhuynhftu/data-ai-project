# create_snowflake_stages.py
import snowflake.connector
from dotenv import load_dotenv
import os

def create_snowflake_stages():
    """Create Snowflake stages for MinIO external storage."""
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
        
        print("🏗️ Creating Snowflake stages for MinIO...")
        
        # Create schemas if they don't exist
        schemas_to_create = ['RAW_DATA', 'STAGING', 'ANALYTICS']
        
        for schema in schemas_to_create:
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                print(f"✅ Schema {schema} created/verified")
            except Exception as e:
                print(f"⚠️ Schema {schema}: {e}")
        
        # Switch to RAW_DATA schema for creating stages
        cursor.execute("USE SCHEMA RAW_DATA")
        
        # Create MinIO external stages
        stages = [
            {
                'name': 'CSV_STAGE',
                'description': 'Stage for CSV files in MinIO',
                'path': 'raw_data/csv/'
            },
            {
                'name': 'JSON_STAGE',
                'description': 'Stage for JSON files in MinIO',
                'path': 'raw_data/json/'
            },
            {
                'name': 'PARQUET_STAGE',
                'description': 'Stage for Parquet files in MinIO',
                'path': 'raw_data/parquet/'
            }
        ]
        
        # MinIO connection details (you'll need to update these)
        minio_url = "http://localhost:9000"  # Update with your MinIO URL
        bucket_name = "raw_data"  # Update with your bucket name
        
        for stage in stages:
            stage_sql = f"""
            CREATE OR REPLACE STAGE {stage['name']}
            URL = '{minio_url}/{bucket_name}/{stage['path']}'
            CREDENTIALS = (
                AWS_KEY_ID = '{os.getenv("MINIO_ACCESS_KEY", "minioadmin")}'
                AWS_SECRET_KEY = '{os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "password"))}'
            )
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                NULL_IF = ('NULL', 'null', '')
                EMPTY_FIELD_AS_NULL = TRUE
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            COMMENT = '{stage['description']}'
            """
            
            try:
                cursor.execute(stage_sql)
                print(f"✅ Stage {stage['name']} created successfully")
            except Exception as e:
                print(f"❌ Error creating stage {stage['name']}: {e}")
        
        # Create file formats for different data types
        file_formats = [
            {
                'name': 'CSV_FORMAT',
                'type': 'CSV',
                'options': """
                    FIELD_DELIMITER = ','
                    SKIP_HEADER = 1
                    NULL_IF = ('NULL', 'null', '')
                    EMPTY_FIELD_AS_NULL = TRUE
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                """
            },
            {
                'name': 'JSON_FORMAT',
                'type': 'JSON',
                'options': """
                    STRIP_OUTER_ARRAY = TRUE
                    NULL_IF = ()
                """
            },
            {
                'name': 'PARQUET_FORMAT',
                'type': 'PARQUET',
                'options': ""
            }
        ]
        
        print("\n🗂️ Creating file formats...")
        for fmt in file_formats:
            format_sql = f"""
            CREATE OR REPLACE FILE FORMAT {fmt['name']}
            TYPE = {fmt['type']}
            {fmt['options']}
            """
            
            try:
                cursor.execute(format_sql)
                print(f"✅ File format {fmt['name']} created successfully")
            except Exception as e:
                print(f"❌ Error creating file format {fmt['name']}: {e}")
        
        # Test the stages
        print("\n🧪 Testing stages...")
        cursor.execute("SHOW STAGES")
        stages_result = cursor.fetchall()
        
        print(f"📊 Found {len(stages_result)} stages:")
        for stage in stages_result:
            print(f"   - {stage[1]}")  # Stage name is in the second column
        
        print("\n✅ Snowflake stages setup completed!")
        
        # Display connection information for MinIO
        print("\n📋 **MinIO Configuration Needed:**")
        print("=" * 50)
        print("1. Make sure MinIO is running on: http://localhost:9000")
        print("2. Create a bucket named: datalake")
        print("3. Add these to your .env file:")
        print("   MINIO_ACCESS_KEY=minioadmin")
        print("   MINIO_SECRET_KEY=password")
        print("4. Folder structure in MinIO bucket:")
        print("   - bronze/")
        print("   - silver/")
        print("   - gold/")
        print("   - raw/csv/")
        print("   - raw/json/")
        print("   - raw/parquet/")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create stages: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_snowflake_stages()