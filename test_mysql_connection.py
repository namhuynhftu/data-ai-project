#!/usr/bin/env python3
"""Test MySQL connection and database setup."""

import os
import sys
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

def test_mysql_connection():
    """Test MySQL connection with current environment variables."""
    load_dotenv()
    
    print("🔍 Testing MySQL connection...")
    print(f"Host: {os.getenv('MYSQL_HOST')}")
    print(f"Port: {os.getenv('MYSQL_PORT')}")
    print(f"Database: {os.getenv('MYSQL_DATABASE')}")
    print(f"User: {os.getenv('MYSQL_ROOT_USER')}")
    print(f"Password: {'***' if os.getenv('MYSQL_ROOT_PASSWORD') else 'NOT SET'}")
    print("=" * 50)
    
    connection = None
    try:
        # Test connection parameters
        connection_config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'user': os.getenv('MYSQL_ROOT_USER', 'root'),
            'password': os.getenv('MYSQL_ROOT_PASSWORD', 'password'),
            'database': os.getenv('MYSQL_DATABASE', 'ecommerce_db'),
            'auth_plugin': 'mysql_native_password'
        }
        
        # Create connection
        print("📡 Attempting to connect...")
        connection = mysql.connector.connect(**connection_config)
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"✅ Connection successful!")
            print(f"🗄️  MySQL Server version: {db_info}")
            
            cursor = connection.cursor()
            
            # Test database access
            cursor.execute("SELECT DATABASE();")
            database_name = cursor.fetchone()
            print(f"📂 Connected to database: {database_name[0]}")
            
            # List all tables
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            print(f"📋 Found {len(tables)} tables:")
            for table in tables:
                print(f"   - {table[0]}")
            
            # Get row counts for each table
            print(f"\n📊 Table row counts:")
            table_stats = []
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                table_stats.append((table_name, count))
                print(f"   - {table_name}: {count:,} rows")
            
            # Test a sample query
            print(f"\n🔍 Testing sample queries:")
            
            # Test customers table
            if any('customers' in table[0] for table in tables):
                cursor.execute("SELECT COUNT(DISTINCT customer_state) FROM olist_customers_dataset;")
                states = cursor.fetchone()[0]
                print(f"   - Unique customer states: {states}")
            
            # Test orders table
            if any('orders' in table[0] for table in tables):
                cursor.execute("SELECT COUNT(DISTINCT order_status) FROM olist_orders_dataset;")
                statuses = cursor.fetchone()[0]
                print(f"   - Unique order statuses: {statuses}")
                
                cursor.execute("SELECT order_status, COUNT(*) as count FROM olist_orders_dataset GROUP BY order_status ORDER BY count DESC LIMIT 3;")
                top_statuses = cursor.fetchall()
                print(f"   - Top order statuses:")
                for status, count in top_statuses:
                    print(f"     • {status}: {count:,}")
            
            cursor.close()
            print(f"\n🎉 All tests passed! MySQL database is working correctly.")
            return True
            
    except Error as e:
        print(f"❌ MySQL Error: {e}")
        
        # Provide specific troubleshooting tips
        if "Access denied" in str(e):
            print("💡 Authentication failed. Check:")
            print("   - MYSQL_ROOT_USER and MYSQL_ROOT_PASSWORD in .env file")
            print("   - MySQL server is running: docker ps")
            print("   - Try connecting with: mysql -h localhost -u root -p")
            
        elif "Can't connect to MySQL server" in str(e):
            print("💡 Connection failed. Check:")
            print("   - MySQL container is running: docker ps")
            print("   - Port 3306 is accessible: docker logs mysql_data_source")
            print("   - Host and port in .env file are correct")
            
        elif "Unknown database" in str(e):
            print("💡 Database not found. Check:")
            print("   - Database name in .env file")
            print("   - MySQL initialization scripts ran successfully")
            print("   - Check container logs: docker logs mysql_data_source")
            
        elif "Public Key Retrieval is not allowed" in str(e):
            print("💡 Authentication plugin issue. Try:")
            print("   - Add allowPublicKeyRetrieval=true to connection")
            print("   - Or use mysql_native_password authentication")
            
        return False
        
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        print("💡 Check that mysql-connector-python is installed:")
        print("   pip install mysql-connector-python")
        return False
        
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("🔌 Connection closed.")

def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import mysql.connector
        print("✅ mysql-connector-python is installed")
        return True
    except ImportError:
        print("❌ mysql-connector-python is not installed")
        print("💡 Install it with: pip install mysql-connector-python")
        return False

if __name__ == "__main__":
    print("🚀 MySQL Connection Test")
    print("=" * 50)
    
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)
    
    # Run the connection test
    success = test_mysql_connection()
    
    if success:
        print("\n✅ MySQL connection test completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ MySQL connection test failed!")
        sys.exit(1)