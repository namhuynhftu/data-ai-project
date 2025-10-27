"""
Snowflake & MinIO Cleanup Script
This script performs complete cleanup:
1. Truncates all tables in RAW_DATA schema
2. Drops STAGING and ANALYTICS schemas
3. Drops MINIO_STAGE_SHARED stage
4. Cleans up MinIO subfolders except raw-data
"""
import os
import snowflake.connector
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime

def cleanup_snowflake():
    """
    Perform Snowflake cleanup:
    - Truncate all tables in RAW_DATA schema
    - Drop STAGING and ANALYTICS schemas
    - Drop MINIO_STAGE_SHARED stage
    """
    
    # Load environment variables
    load_dotenv()
    
    # Connection parameters
    params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'private_key_file': os.getenv('SNOWFLAKE_PRIVATE_KEY_FILE_PATH'),
        'private_key_file_pwd': os.getenv('SNOWFLAKE_PRIVATE_KEY_FILE_PWD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'authenticator': 'SNOWFLAKE_JWT'
    }
    
    print("🧹 Snowflake Cleanup Tool")
    print("=" * 60)
    print(f"Database: {params['database']}")
    print(f"User: {params['user']}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()
        print("✅ Connected to Snowflake successfully!\n")
        
        # STEP 1: Truncate all tables in RAW_DATA schema
        print("📋 STEP 1: Truncating all tables in RAW_DATA schema...")
        print("-" * 60)
        try:
            cursor.execute(f"USE SCHEMA {params['database']}.RAW_DATA")
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            if tables:
                print(f"   Found {len(tables)} table(s) in RAW_DATA:")
                truncated = 0
                failed = 0
                
                for table in tables:
                    table_name = table[1]
                    try:
                        cursor.execute(f"TRUNCATE TABLE {params['database']}.RAW_DATA.{table_name}")
                        print(f"      ✅ Truncated: {table_name}")
                        truncated += 1
                    except Exception as e:
                        print(f"      ❌ Failed to truncate {table_name}: {e}")
                        failed += 1
                
                print(f"   📊 RAW_DATA: {truncated} truncated, {failed} failed")
            else:
                print("   ℹ️  No tables found in RAW_DATA schema")
        except Exception as e:
            print(f"   ⚠️  Error processing RAW_DATA schema: {e}")
        
        # STEP 2: Drop STAGING schema
        print(f"\n📋 STEP 2: Dropping STAGING schema...")
        print("-" * 60)
        try:
            cursor.execute(f"DROP SCHEMA IF EXISTS {params['database']}.STAGING CASCADE")
            print("   ✅ STAGING schema dropped successfully")
        except Exception as e:
            print(f"   ⚠️  Error dropping STAGING schema: {e}")
        
        # STEP 3: Drop ANALYTICS schema
        print(f"\n📋 STEP 3: Dropping ANALYTICS schema...")
        print("-" * 60)
        try:
            cursor.execute(f"DROP SCHEMA IF EXISTS {params['database']}.ANALYTICS CASCADE")
            print("   ✅ ANALYTICS schema dropped successfully")
        except Exception as e:
            print(f"   ⚠️  Error dropping ANALYTICS schema: {e}")
        
        # STEP 4: Drop MINIO_STAGE_SHARED stage
        print(f"\n📋 STEP 4: Dropping MINIO_STAGE_SHARED stage...")
        print("-" * 60)
        try:
            cursor.execute(f"USE SCHEMA {params['database']}.RAW_DATA")
            cursor.execute(f"DROP STAGE IF EXISTS {params['database']}.RAW_DATA.MINIO_STAGE_SHARED")
            print("   ✅ MINIO_STAGE_SHARED stage dropped successfully")
        except Exception as e:
            print(f"   ⚠️  Error dropping MINIO_STAGE_SHARED stage: {e}")
        
        print(f"\n" + "=" * 60)
        print("✅ Snowflake cleanup completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Snowflake operation failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if 'conn' in locals():
            conn.close()
            print("✅ Snowflake connection closed\n")


def cleanup_minio():
    """
    Clean up MinIO buckets - remove all subfolders except raw-data
    """
    load_dotenv()
    
    print("🧹 MinIO Cleanup Tool")
    print("=" * 60)
    
    # Parse MinIO endpoint
    endpoint = os.getenv("MINIO_ENDPOINT")
    if endpoint and ":" in endpoint:
        host, port = endpoint.split(":", 1)
    else:
        host = os.getenv("MINIO_HOST", "localhost")
        port = os.getenv("MINIO_PORT", "9000")
    
    bucket_name = os.getenv("MINIO_BUCKET", "data-lake")
    
    print(f"MinIO Endpoint: {host}:{port}")
    print(f"Bucket: {bucket_name}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Create MinIO client
        minio_client = Minio(
            f"{host}:{port}",
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )
        
        print("✅ Connected to MinIO successfully!\n")
        
        # Check if bucket exists
        if not minio_client.bucket_exists(bucket_name):
            print(f"⚠️  Bucket '{bucket_name}' does not exist")
            return
        
        print(f"📋 Listing objects in bucket '{bucket_name}'...")
        print("-" * 60)
        
        # List all objects in the bucket
        objects = list(minio_client.list_objects(bucket_name, recursive=True))
        
        if not objects:
            print("   ℹ️  No objects found in bucket")
            return
        
        print(f"   Found {len(objects)} object(s)")
        
        # Filter objects to delete (exclude raw-data folder)
        objects_to_delete = []
        objects_to_keep = []
        
        for obj in objects:
            if obj.object_name.startswith("raw-data/"):
                objects_to_keep.append(obj.object_name)
            else:
                objects_to_delete.append(obj.object_name)
        
        print(f"\n📊 Analysis:")
        print(f"   Objects to keep (raw-data/*): {len(objects_to_keep)}")
        print(f"   Objects to delete: {len(objects_to_delete)}")
        
        if objects_to_delete:
            print(f"\n�️  Deleting {len(objects_to_delete)} object(s)...")
            print("-" * 60)
            
            deleted = 0
            failed = 0
            
            for obj_name in objects_to_delete:
                try:
                    minio_client.remove_object(bucket_name, obj_name)
                    print(f"   ✅ Deleted: {obj_name}")
                    deleted += 1
                except Exception as e:
                    print(f"   ❌ Failed to delete {obj_name}: {e}")
                    failed += 1
            
            print(f"\n📊 Deletion summary: {deleted} deleted, {failed} failed")
        else:
            print("\n   ℹ️  No objects to delete (only raw-data exists)")
        
        print(f"\n" + "=" * 60)
        print("✅ MinIO cleanup completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ MinIO operation failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🧹 COMPLETE CLEANUP TOOL")
    print("=" * 60)
    print("This script will:")
    print("  1. Truncate all tables in RAW_DATA schema")
    print("  2. Drop STAGING schema")
    print("  3. Drop ANALYTICS schema")
    print("  4. Drop MINIO_STAGE_SHARED stage")
    print("  5. Clean up MinIO (except raw-data folder)")
    print("=" * 60)
    
    # Ask for confirmation
    confirmation = input("\n⚠️  Are you sure you want to proceed? (yes/no): ").strip().lower()
    
    if confirmation != "yes":
        print("\n❌ Cleanup cancelled by user")
        exit(0)
    
    print("\n🚀 Starting cleanup process...\n")
    
    # Run Snowflake cleanup
    cleanup_snowflake()
    
    # Run MinIO cleanup
    cleanup_minio()
    
    print("\n" + "=" * 60)
    print("✅ ALL CLEANUP OPERATIONS COMPLETED!")
    print("=" * 60)