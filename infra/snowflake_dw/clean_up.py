"""
Snowflake Database Cleanup Script
This script truncates all tables in all schemas of the database to clean up data.
"""
import os
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

def truncate_all_tables():
    """Truncate all tables in all schemas of the Snowflake database."""
    
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
    
    print("🧹 Snowflake Database Cleanup Tool")
    print("=" * 60)
    print(f"Database: {params['database']}")
    print(f"User: {params['user']}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    try:
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()
        print("✅ Connected to Snowflake successfully!")
        
        # 1. Get all schemas in the database (exclude system schemas)
        print(f"\n📋 Step 1: Finding all schemas in database '{params['database']}'...")
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()
        
        # Filter out system schemas
        user_schemas = []
        system_schemas = ['INFORMATION_SCHEMA', 'PUBLIC']
        
        for schema in schemas:
            schema_name = schema[1]
            if schema_name not in system_schemas:
                user_schemas.append(schema_name)
                
        print(f"   Found {len(user_schemas)} user schemas to process:")
        for schema_name in user_schemas:
            print(f"      - {schema_name}")
        
        if not user_schemas:
            print("   ℹ️  No user schemas found to clean")
            return
            
        # 2. Process each schema
        total_tables_found = 0
        total_tables_truncated = 0
        total_tables_failed = 0
        
        for schema_name in user_schemas:
            print(f"\n📊 Step 2: Processing schema '{schema_name}'...")
            
            try:
                # Switch to the schema
                cursor.execute(f"USE SCHEMA {params['database']}.{schema_name}")
                
                # Get all tables in the schema
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                if not tables:
                    print(f"   ℹ️  No tables found in schema '{schema_name}'")
                    continue
                    
                print(f"   📋 Found {len(tables)} table(s) in '{schema_name}':")
                schema_tables_truncated = 0
                schema_tables_failed = 0
                
                for table in tables:
                    table_name = table[1]
                    total_tables_found += 1
                    
                    try:
                        # Truncate the table
                        truncate_sql = f"TRUNCATE TABLE {params['database']}.{schema_name}.{table_name}"
                        cursor.execute(truncate_sql)
                        
                        print(f"      ✅ Truncated: {table_name}")
                        total_tables_truncated += 1
                        schema_tables_truncated += 1
                        
                    except Exception as e:
                        print(f"      ❌ Failed to truncate {table_name}: {e}")
                        total_tables_failed += 1
                        schema_tables_failed += 1
                
                print(f"   📊 Schema '{schema_name}' summary: {schema_tables_truncated} truncated, {schema_tables_failed} failed")
                
            except Exception as e:
                print(f"   ❌ Error processing schema '{schema_name}': {e}")
        
        # 3. Final summary
        print(f"\n" + "=" * 60)
        print("🧹 CLEANUP COMPLETED")
        print("=" * 60)
        print("📊 SUMMARY:")
        print(f"   Schemas processed: {len(user_schemas)}")
        print(f"   Tables found: {total_tables_found}")
        print(f"   Tables truncated: {total_tables_truncated}")
        print(f"   Tables failed: {total_tables_failed}")
        
        if total_tables_truncated > 0:
            success_rate = (total_tables_truncated / total_tables_found) * 100 if total_tables_found > 0 else 0
            print(f"   Success rate: {success_rate:.1f}%")
        
        if total_tables_truncated > 0:
            print(f"\n✅ Successfully cleaned {total_tables_truncated} tables!")
        else:
            print(f"\n⚠️  No tables were truncated")
            
        print("=" * 60)
        
        return {
            'schemas_processed': len(user_schemas),
            'tables_found': total_tables_found,
            'tables_truncated': total_tables_truncated,
            'tables_failed': total_tables_failed,
            'success': total_tables_failed == 0
        }
        
    except Exception as e:
        print(f"❌ Database connection or operation failed: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}
        
    finally:
        if 'conn' in locals():
            conn.close()
            print(f"\n✅ Database connection closed")


def truncate_specific_schema(schema_name: str):
    """Truncate all tables in a specific schema only."""
    
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
    
    print(f"🧹 Cleaning schema: {schema_name}")
    print("-" * 40)
    
    try:
        conn = snowflake.connector.connect(**params)
        cursor = conn.cursor()
        
        # Switch to the schema
        cursor.execute(f"USE SCHEMA {params['database']}.{schema_name}")
        
        # Get all tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if not tables:
            print(f"ℹ️  No tables found in schema '{schema_name}'")
            return
            
        print(f"📋 Truncating {len(tables)} table(s):")
        
        truncated = 0
        failed = 0
        
        for table in tables:
            table_name = table[1]
            try:
                cursor.execute(f"TRUNCATE TABLE {params['database']}.{schema_name}.{table_name}")
                print(f"   ✅ {table_name}")
                truncated += 1
            except Exception as e:
                print(f"   ❌ {table_name}: {e}")
                failed += 1
        
        print(f"\n📊 Results: {truncated} truncated, {failed} failed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    print("🧹 Snowflake RAW_DATA Schema Cleanup")
    print("=" * 50)
    print("This script will truncate ALL tables in the RAW_DATA schema")
    print("-" * 50)
    
    # Directly run RAW_DATA schema cleanup
    truncate_specific_schema("RAW_DATA")