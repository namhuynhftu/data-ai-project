#!/usr/bin/env python3
"""Test MinIO connection and credentials."""

import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

def test_minio_connection():
    """Test MinIO connection with current environment variables."""
    load_dotenv()
    
    print("🔍 Testing MinIO connection...")
    print(f"Endpoint: {os.getenv('MINIO_ENDPOINT')}")
    print(f"Access Key: {os.getenv('MINIO_ROOT_USER')}")
    print(f"Secret Key: {'***' if os.getenv('MINIO_ROOT_PASSWORD') else 'NOT SET'}")
    
    try:
        client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            access_key=os.getenv('MINIO_ROOT_USER', 'user'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            secure=False
        )
        
        # Test connection by listing buckets
        buckets = list(client.list_buckets())
        print(f"✅ Connection successful!")
        print(f"📦 Found {len(buckets)} buckets:")
        for bucket in buckets:
            print(f"   - {bucket.name} (created: {bucket.creation_date})")
        
        # Test bucket creation if raw-data doesn't exist
        bucket_name = "raw-data"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✅ Created bucket: {bucket_name}")
        else:
            print(f"✅ Bucket '{bucket_name}' already exists")
        
        return True
        
    except S3Error as e:
        print(f"❌ MinIO S3 Error: {e}")
        if e.code == "AccessDenied":
            print("💡 Check your MinIO credentials:")
            print("   - MINIO_ROOT_USER should be 'minioadmin' (default)")
            print("   - MINIO_ROOT_PASSWORD should be 'minioadmin' (default)")
        elif e.code == "InvalidAccessKeyId":
            print("💡 Access key is incorrect")
        elif e.code == "SignatureDoesNotMatch":
            print("💡 Secret key is incorrect")
        return False
        
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        if "path in endpoint is not allowed" in str(e):
            print("💡 Remove 'http://' from MINIO_ENDPOINT")
        return False

if __name__ == "__main__":
    test_minio_connection()