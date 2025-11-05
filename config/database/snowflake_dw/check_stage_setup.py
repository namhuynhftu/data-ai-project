"""
Test script to check Snowflake stages in RAW_DATA schema.
This script connects to your Snowflake database and lists all stages.
"""
import os
import snowflake.connector
from dotenv import load_dotenv

def check_snowflake_stages():
    """Connect to Snowflake and check all stages in RAW_DATA schema."""
    
    # Load environment variables
    load_dotenv()
    
    # Connection parameters using your environment variables
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
    
    print("Snowflake Connection Parameters:")
    print(f"  Account: {params['account']}")
    print(f"  User: {params['user']}")
    print(f"  Role: {params['role']}")
    print(f"  Database: {params['database']}")
    print(f"  Schema: {params['schema']}")
    print(f"  Warehouse: {params['warehouse']}")
    print("-" * 60)
    
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()
        print("✅ Connection successful!")
        
        # Query all stages in the current schema
        print(f"\nQuerying stages in {params['schema']} schema...")
        cursor.execute("SHOW STAGES")
        stages = cursor.fetchall()
        
        print(f"\n📊 RESULT: Found {len(stages)} stage(s) in {params['schema']} schema")
        
        if stages:
            print("\n📋 Stage Details:")
            print("=" * 60)
            
            for i, stage in enumerate(stages, 1):
                stage_name = stage[1]
                stage_url = stage[2] if stage[2] else "Internal Stage"
                has_credentials = stage[3] if len(stage) > 3 else "Unknown"
                has_encryption = stage[4] if len(stage) > 4 else "Unknown"
                stage_type = "External" if stage[2] else "Internal"
                
                print(f"\n{i}. Stage Name: {stage_name}")
                print(f"   Type: {stage_type}")
                print(f"   URL/Location: {stage_url}")
                print(f"   Has Credentials: {has_credentials}")
                print(f"   Has Encryption: {has_encryption}")
                
                if stage_name == 'MINIO_STAGE_SHARED':
                    print("   🎯 ** This is our pipeline's MinIO stage **")
        else:
            print(f"\n❌ No stages found in {params['schema']} schema")
        
        # Check specifically for MINIO_STAGE_SHARED
        print("\n" + "=" * 60)
        print("🔍 Checking for MINIO_STAGE_SHARED specifically...")
        cursor.execute("SHOW STAGES LIKE 'MINIO_STAGE_SHARED'")
        minio_stage = cursor.fetchall()
        
        if minio_stage:
            print("✅ MINIO_STAGE_SHARED exists and is configured")
            print("🚀 Pipeline Status: Ready for direct MinIO-Snowflake integration")
        else:
            print("⚠️  MINIO_STAGE_SHARED not found")
            print("🔄 Pipeline Status: Using temp file fallback method (current working approach)")
        
        # Summary
        print(f"\n📝 SUMMARY:")
        print(f"   Total stages: {len(stages)}")
        stage_names = [stage[1] for stage in stages] if stages else []
        print(f"   Stage names: {stage_names}")
        
        # Check current warehouse and compute status
        cursor.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
        current_info = cursor.fetchone()
        print(f"\n🔧 Current Session Info:")
        print(f"   Warehouse: {current_info[0]}")
        print(f"   Database: {current_info[1]}")
        print(f"   Schema: {current_info[2]}")
        print(f"   Role: {current_info[3]}")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔧 Troubleshooting tips:")
        print("1. Check if your Snowflake credentials are correct")
        print("2. Verify your private key file exists and password is correct")
        print("3. Ensure your role has access to the database and schema")
        return False
        
    finally:
        if 'conn' in locals():
            conn.close()
            print("\n✅ Connection closed successfully")
    
    return True

def delete_all_stages():
    """Delete all stages in the RAW_DATA schema."""
    
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
    
    try:
        print("🗑️  Connecting to Snowflake to delete stages...")
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()
        print("✅ Connected successfully!")
        
        # First, get all existing stages
        cursor.execute("SHOW STAGES")
        stages = cursor.fetchall()
        
        if not stages:
            print("ℹ️  No stages found to delete.")
            return True
        
        print(f"\n📋 Found {len(stages)} stage(s) to delete:")
        stage_names = []
        for stage in stages:
            stage_name = stage[1]
            stage_names.append(stage_name)
            print(f"   - {stage_name}")
        
        # Confirm deletion
        print(f"\n⚠️  WARNING: This will delete ALL {len(stages)} stages in {params['schema']} schema!")
        print("🔥 This action cannot be undone!")
        
        # Delete each stage
        print(f"\n🗑️  Deleting stages...")
        deleted_count = 0
        failed_count = 0
        
        for stage_name in stage_names:
            try:
                drop_sql = f"DROP STAGE IF EXISTS {stage_name}"
                cursor.execute(drop_sql)
                print(f"✅ Deleted: {stage_name}")
                deleted_count += 1
            except Exception as e:
                print(f"❌ Failed to delete {stage_name}: {e}")
                failed_count += 1
        
        # Summary
        print(f"\n� DELETION SUMMARY:")
        print(f"   ✅ Successfully deleted: {deleted_count}")
        print(f"   ❌ Failed to delete: {failed_count}")
        print(f"   📋 Total stages processed: {len(stage_names)}")
        
        # Verify deletion
        print(f"\n�🔍 Verifying deletion...")
        cursor.execute("SHOW STAGES")
        remaining_stages = cursor.fetchall()
        
        if remaining_stages:
            print(f"⚠️  {len(remaining_stages)} stage(s) still exist:")
            for stage in remaining_stages:
                print(f"   - {stage[1]}")
        else:
            print("✅ All stages have been successfully deleted!")
        
        return deleted_count > 0
        
    except Exception as e:
        print(f"❌ Error during stage deletion: {e}")
        return False
        
    finally:
        if 'conn' in locals():
            conn.close()
            print("\n✅ Connection closed")


if __name__ == "__main__":
    print("🔍 Snowflake Stages Manager")
    print("=" * 60)
    
    print("Choose an option:")
    print("1. Check stages (view only)")
    print("2. Delete ALL stages (destructive operation)")
    print()
    
    choice = input("Enter your choice (1 or 2): ").strip()
    
    if choice == "1":
        print("\n📋 Checking stages...")
        check_snowflake_stages()
    elif choice == "2":
        print("\n🗑️  Stage deletion mode...")
        confirm = input("⚠️  Are you ABSOLUTELY sure you want to delete ALL stages? (type 'DELETE' to confirm): ").strip()
        if confirm == "DELETE":
            delete_all_stages()
        else:
            print("❌ Deletion cancelled. Stages are safe.")
    else:
        print("❌ Invalid choice. Please run again and select 1 or 2.")