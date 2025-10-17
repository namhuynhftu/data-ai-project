#!/usr/bin/env python3
"""Test PostgreSQL connection and database setup."""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error, sql

def test_postgresql_connection():
    """Test PostgreSQL connection with current environment variables."""
    load_dotenv()
    
    print("🔍 Testing PostgreSQL connection...")
    print(f"Host: {os.getenv('POSTGRES_HOST')}")
    print(f"Port: {os.getenv('POSTGRES_PORT')}")
    print(f"Database: {os.getenv('POSTGRES_DB')}")
    print(f"User: {os.getenv('POSTGRES_USER')}")
    print(f"Password: {'***' if os.getenv('POSTGRES_PASSWORD') else 'NOT SET'}")
    print("=" * 50)
    
    connection = None
    try:
        # Test connection parameters
        connection_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'user': os.getenv('POSTGRES_USER', 'user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'password'),
            'database': os.getenv('POSTGRES_DB', 'streaming_db')
        }
        
        # Create connection
        print("📡 Attempting to connect...")
        connection = psycopg2.connect(**connection_config)
        
        # Set autocommit for administrative queries
        connection.autocommit = True
        cursor = connection.cursor()
        
        print(f"✅ Connection successful!")
        
        # Get server version
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()[0]
        print(f"🗄️  PostgreSQL version: {db_version}")
        
        # Get current database
        cursor.execute("SELECT current_database();")
        database_name = cursor.fetchone()[0]
        print(f"📂 Connected to database: {database_name}")
        
        # Get current timezone
        cursor.execute("SHOW timezone;")
        timezone = cursor.fetchone()[0]
        print(f"🌍 Database timezone: {timezone}")
        
        # List all schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name;
        """)
        schemas = cursor.fetchall()
        print(f"📁 Found {len(schemas)} custom schemas:")
        for schema in schemas:
            print(f"   - {schema[0]}")
        
        # List all tables in streaming schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'streaming'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        print(f"📋 Found {len(tables)} tables in 'streaming' schema:")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Get row counts for each table
        if tables:
            print(f"\n📊 Table row counts in 'streaming' schema:")
            table_stats = []
            for table in tables:
                table_name = table[0]
                try:
                    cursor.execute(sql.SQL("SELECT COUNT(*) FROM streaming.{}").format(
                        sql.Identifier(table_name)
                    ))
                    count = cursor.fetchone()[0]
                    table_stats.append((table_name, count))
                    print(f"   - {table_name}: {count:,} rows")
                except Exception as e:
                    print(f"   - {table_name}: Error counting rows - {e}")
        
        # Test some sample queries if data exists
        print(f"\n🔍 Testing sample queries:")
        
        # Check if users table has data
        try:
            cursor.execute("SELECT COUNT(*) FROM streaming.users;")
            user_count = cursor.fetchone()[0]
            if user_count > 0:
                cursor.execute("SELECT COUNT(DISTINCT email) FROM streaming.users;")
                unique_emails = cursor.fetchone()[0]
                print(f"   - Total users: {user_count:,}")
                print(f"   - Unique emails: {unique_emails:,}")
            else:
                print(f"   - Users table is empty")
        except Exception as e:
            print(f"   - Could not query users table: {e}")
        
        # Check if transactions table has data
        try:
            cursor.execute("SELECT COUNT(*) FROM streaming.transactions;")
            transaction_count = cursor.fetchone()[0]
            if transaction_count > 0:
                cursor.execute("SELECT COUNT(DISTINCT currency) FROM streaming.transactions;")
                currencies = cursor.fetchone()[0]
                cursor.execute("SELECT currency, COUNT(*) as count FROM streaming.transactions GROUP BY currency ORDER BY count DESC LIMIT 3;")
                top_currencies = cursor.fetchall()
                print(f"   - Total transactions: {transaction_count:,}")
                print(f"   - Unique currencies: {currencies}")
                print(f"   - Top currencies:")
                for currency, count in top_currencies:
                    print(f"     • {currency}: {count:,}")
            else:
                print(f"   - Transactions table is empty")
        except Exception as e:
            print(f"   - Could not query transactions table: {e}")
        
        # Check detailed transactions if exists
        try:
            cursor.execute("SELECT COUNT(*) FROM streaming.detailed_transactions;")
            detailed_count = cursor.fetchone()[0]
            if detailed_count > 0:
                cursor.execute("SELECT COUNT(DISTINCT transaction_type) FROM streaming.detailed_transactions;")
                types = cursor.fetchone()[0]
                print(f"   - Detailed transactions: {detailed_count:,}")
                print(f"   - Transaction types: {types}")
            else:
                print(f"   - Detailed transactions table is empty")
        except Exception as e:
            print(f"   - Detailed transactions table may not exist: {e}")
        
        cursor.close()
        print(f"\n🎉 All tests passed! PostgreSQL database is working correctly.")
        return True
        
    except Error as e:
        print(f"❌ PostgreSQL Error: {e}")
        
        # Provide specific troubleshooting tips
        if "authentication failed" in str(e).lower():
            print("💡 Authentication failed. Check:")
            print("   - POSTGRES_USER and POSTGRES_PASSWORD in .env file")
            print("   - PostgreSQL server is running: docker ps")
            
        elif "could not connect to server" in str(e).lower():
            print("💡 Connection failed. Check:")
            print("   - PostgreSQL container is running: docker ps")
            print("   - Port 5432 is accessible: docker logs postgres_dw")
            print("   - Host and port in .env file are correct")
            
        elif "database" in str(e).lower() and "does not exist" in str(e).lower():
            print("💡 Database not found. Check:")
            print("   - Database name in .env file")
            print("   - PostgreSQL initialization scripts ran successfully")
            print("   - Check container logs: docker logs postgres_dw")
            
        elif "timezone" in str(e).lower() or "TimeZone" in str(e):
            print("💡 Timezone issue. Check:")
            print("   - PostgreSQL timezone configuration")
            print("   - System timezone settings")
            print("   - Use 'Asia/Ho_Chi_Minh' instead of 'Asia/Saigon'")
            
        return False
        
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        print("💡 Check that psycopg2 is installed:")
        print("   pip install psycopg2-binary")
        return False
        
    finally:
        if connection:
            connection.close()
            print("🔌 Connection closed.")

def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import psycopg2
        print("✅ psycopg2 is installed")
        return True
    except ImportError:
        print("❌ psycopg2 is not installed")
        print("💡 Install it with: pip install psycopg2-binary")
        return False

if __name__ == "__main__":
    print("🚀 PostgreSQL Connection Test")
    print("=" * 50)
    
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)
    
    # Run the connection test
    success = test_postgresql_connection()
    
    if success:
        print("\n✅ PostgreSQL connection test completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ PostgreSQL connection test failed!")
        sys.exit(1)