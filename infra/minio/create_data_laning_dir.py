import os

from  dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error



# Directory paths
BATCH_PATH = "batch"
STREAMING_PATH = "streaming"

def create_directory(client: Minio, bucket_name: str, dir_path: str):
    """Create a directory in the specified MinIO bucket."""
    try:
        # Ensure the bucket exists
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket created: {bucket_name}")
        else:
            print(f"Bucket already exists: {bucket_name}")
        
        # Create a placeholder object to represent the directory
        dir_object = dir_path.rstrip('/') + '/'
        client.put_object(bucket_name, dir_object, content_type='application/data')
        print(f"Directory created: {dir_path} in bucket: {bucket_name}")
        
    except S3Error as e:
        print(f"Error creating directory {dir_path} in bucket {bucket_name}: {e}")
        raise

def main():
    load_dotenv()
    client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    create_directory(client, os.getenv("MINIO_BUCKET"), BATCH_PATH)
    # create_directory(client, os.getenv("MINIO_BUCKET"), STREAMING_PATH)

if __name__ == "__main__":
    main()