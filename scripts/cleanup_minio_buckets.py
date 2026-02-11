"""
MinIO Bucket Cleanup Utility
=============================
Remove unnecessary or empty buckets from MinIO data lake.

Usage:
    python cleanup_minio_buckets.py --list              # List all buckets
    python cleanup_minio_buckets.py --dry-run           # Show what would be deleted
    python cleanup_minio_buckets.py --remove-empty      # Remove empty buckets
    python cleanup_minio_buckets.py --remove <bucket>   # Remove specific bucket
    python cleanup_minio_buckets.py --remove-all        # Remove all buckets (careful!)

Author: Nam Huynh
Date: January 2026
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv


# ============================================================================
# CONFIGURATION
# ============================================================================
# Load environment variables
project_root = Path(__file__).parent.parent.parent
env_path = project_root / "config" / "app" / "development.env"
load_dotenv(env_path)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Buckets that should never be deleted (protect production data)
PROTECTED_BUCKETS = [
    "data-lake",           # Main data lake bucket
    "cdc-events",     # Transformed data
    "airflow-logs",       # Airflow task logs
]


# ============================================================================
# MINIO CLIENT
# ============================================================================
def get_minio_client() -> Minio:
    """Create and return MinIO client instance."""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        return client
    except Exception as e:
        print(f"❌ Error connecting to MinIO: {e}")
        sys.exit(1)


# ============================================================================
# BUCKET OPERATIONS
# ============================================================================
def list_all_buckets(client: Minio) -> list:
    """List all buckets in MinIO with their details."""
    try:
        buckets = client.list_buckets()
        bucket_info = []
        
        for bucket in buckets:
            # Count objects in bucket
            try:
                objects = list(client.list_objects(bucket.name, recursive=True))
                object_count = len(objects)
                
                # Calculate total size
                total_size = sum(obj.size for obj in objects if obj.size)
                size_mb = total_size / (1024 * 1024)  # Convert to MB
                
            except Exception as e:
                object_count = 0
                size_mb = 0.0
            
            bucket_info.append({
                "name": bucket.name,
                "created": bucket.creation_date,
                "object_count": object_count,
                "size_mb": size_mb,
                "protected": bucket.name in PROTECTED_BUCKETS
            })
        
        return bucket_info
    
    except S3Error as e:
        print(f"❌ Error listing buckets: {e}")
        return []


def print_bucket_list(buckets: list):
    """Print formatted bucket list."""
    if not buckets:
        print("📭 No buckets found in MinIO.")
        return
    
    print("\n" + "="*80)
    print(f"{'Bucket Name':<30} {'Objects':<10} {'Size (MB)':<12} {'Status':<15}")
    print("="*80)
    
    for bucket in buckets:
        protected = "🔒 PROTECTED" if bucket["protected"] else "✅ Available"
        size_str = f"{bucket['size_mb']:.2f}"
        
        print(f"{bucket['name']:<30} {bucket['object_count']:<10} {size_str:<12} {protected:<15}")
    
    print("="*80)
    print(f"Total buckets: {len(buckets)}")
    print(f"Empty buckets: {sum(1 for b in buckets if b['object_count'] == 0)}")
    print(f"Protected buckets: {sum(1 for b in buckets if b['protected'])}\n")


def remove_bucket(client: Minio, bucket_name: str, force: bool = False, quiet: bool = False) -> bool:
    """
    Remove a bucket from MinIO.
    
    Args:
        client: MinIO client instance
        bucket_name: Name of bucket to remove
        force: If True, remove even if bucket has objects
        quiet: If True, suppress verbose output (for batch operations)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if bucket is protected
        if bucket_name in PROTECTED_BUCKETS:
            if not quiet:
                print(f"🔒 Bucket '{bucket_name}' is protected and cannot be removed.")
            return False
        
        # Check if bucket exists
        if not client.bucket_exists(bucket_name):
            if not quiet:
                print(f"❌ Bucket '{bucket_name}' does not exist.")
            return False
        
        # Check if bucket has objects
        objects = list(client.list_objects(bucket_name, recursive=True))
        
        if objects and not force:
            if not quiet:
                print(f"⚠️  Bucket '{bucket_name}' has {len(objects)} objects.")
                print("   Use --force flag to remove non-empty buckets.")
            return False
        
        # Remove all objects if force is True
        if objects and force:
            if not quiet:
                print(f"  Removing {len(objects)} objects...", end=" ")
            for obj in objects:
                client.remove_object(bucket_name, obj.object_name)
        
        # Remove bucket
        client.remove_bucket(bucket_name)
        if not quiet:
            print(f"✅")
        return True
    
    except S3Error as e:
        if not quiet:
            print(f"❌ Error: {e}")
        return False


def remove_empty_buckets(client: Minio, buckets: list, dry_run: bool = False) -> int:
    """
    Remove all empty buckets (excluding protected ones).
    
    Args:
        client: MinIO client instance
        buckets: List of bucket information
        dry_run: If True, only show what would be deleted
    
    Returns:
        Number of buckets removed
    """
    empty_buckets = [
        b for b in buckets 
        if b["object_count"] == 0 and not b["protected"]
    ]
    
    if not empty_buckets:
        print("📭 No empty buckets to remove.")
        return 0
    
    print(f"\n🗑️  Found {len(empty_buckets)} empty bucket(s):")
    for bucket in empty_buckets:
        print(f"   - {bucket['name']}")
    
    if dry_run:
        print("\n[DRY RUN] No buckets were actually removed.")
        return 0
    
    print("\n⚠️  About to remove empty buckets...")
    confirmation = input("Are you sure? (yes/no): ").strip().lower()
    
    if confirmation != "yes":
        print("❌ Operation cancelled.")
        return 0
    
    removed_count = 0
    for bucket in empty_buckets:
        if remove_bucket(client, bucket["name"]):
            removed_count += 1
    
    print(f"\n✅ Removed {removed_count} empty bucket(s).")
    return removed_count


def remove_all_buckets(client: Minio, buckets: list, dry_run: bool = False) -> int:
    """
    Remove ALL buckets (excluding protected ones).
    
    Args:
        client: MinIO client instance
        buckets: List of bucket information
        dry_run: If True, only show what would be deleted
    
    Returns:
        Number of buckets removed
    """
    removable_buckets = [b for b in buckets if not b["protected"]]
    
    if not removable_buckets:
        print("📭 No removable buckets found.")
        return 0
    
    print(f"\n⚠️  WARNING: About to remove {len(removable_buckets)} bucket(s):")
    for bucket in removable_buckets:
        status = f"({bucket['object_count']} objects, {bucket['size_mb']:.2f} MB)"
        print(f"   - {bucket['name']} {status}")
    
    if dry_run:
        print("\n[DRY RUN] No buckets were actually removed.")
        return 0
    
    print("\n🚨 THIS WILL DELETE ALL DATA IN THESE BUCKETS!")
    confirmation = input("Type 'DELETE ALL' to confirm: ").strip()
    
    if confirmation != "DELETE ALL":
        print("❌ Operation cancelled.")
        return 0
    
    removed_count = 0
    for bucket in removable_buckets:
        if remove_bucket(client, bucket["name"], force=True):
            removed_count += 1
    
    print(f"\n✅ Removed {removed_count} bucket(s).")
    return removed_count


def interactive_remove_buckets(client: Minio, buckets: list) -> int:
    """
    Interactive mode to select and remove buckets.
    
    Args:
        client: MinIO client instance
        buckets: List of bucket information
    
    Returns:
        Number of buckets removed
    """
    removable_buckets = [b for b in buckets if not b["protected"]]
    
    if not removable_buckets:
        print("📭 No removable buckets found.")
        return 0
    
    # Display buckets with numbers
    print("\n" + "="*80)
    print("Available Buckets for Removal")
    print("="*80)
    print(f"{'#':<4} {'Bucket Name':<30} {'Objects':<10} {'Size (MB)':<12} {'Status':<10}")
    print("="*80)
    
    for idx, bucket in enumerate(removable_buckets, start=1):
        size_str = f"{bucket['size_mb']:.2f}"
        status = "Empty" if bucket['object_count'] == 0 else "Has data"
        print(f"{idx:<4} {bucket['name']:<30} {bucket['object_count']:<10} {size_str:<12} {status:<10}")
    
    print("="*80)
    print(f"\nTotal removable buckets: {len(removable_buckets)}")
    print(f"Protected buckets (cannot be removed): {', '.join(PROTECTED_BUCKETS)}")
    print("\n" + "-"*80)
    print("Select buckets to remove:")
    print("  • Enter numbers separated by commas (e.g., 1,3,5)")
    print("  • Enter 'all' to select all buckets")
    print("  • Enter 'q' or 'quit' to exit without removing anything")
    print("-"*80 + "\n")
    
    selection = input("Your selection: ").strip().lower()
    
    if selection in ['q', 'quit']:
        print("❌ Operation cancelled.")
        return 0
    
    # Determine which buckets to remove
    selected_buckets = []
    
    if selection == 'all':
        selected_buckets = removable_buckets
    else:
        try:
            indices = [int(x.strip()) for x in selection.split(',')]
            for idx in indices:
                if 1 <= idx <= len(removable_buckets):
                    selected_buckets.append(removable_buckets[idx - 1])
                else:
                    print(f"⚠️  Invalid index: {idx} (must be between 1 and {len(removable_buckets)})")
        except ValueError:
            print("❌ Invalid input. Please enter numbers separated by commas.")
            return 0
    
    if not selected_buckets:
        print("❌ No valid buckets selected.")
        return 0
    
    # Show selected buckets with detailed info
    print("\n" + "="*80)
    print(f"CONFIRMATION: You selected {len(selected_buckets)} bucket(s) for removal")
    print("="*80)
    
    total_objects = 0
    total_size_mb = 0.0
    
    for idx, bucket in enumerate(selected_buckets, start=1):
        total_objects += bucket['object_count']
        total_size_mb += bucket['size_mb']
        status = f"({bucket['object_count']} objects, {bucket['size_mb']:.2f} MB)"
        print(f"  {idx}. {bucket['name']:<30} {status}")
    
    print("="*80)
    print(f"Total data to be deleted: {total_objects} objects, {total_size_mb:.2f} MB")
    print("="*80)
    
    # First confirmation
    print("\n⚠️  WARNING: This action will PERMANENTLY DELETE all data in these buckets!")
    print("⚠️  This operation CANNOT be undone!")
    first_confirm = input("\nDo you want to proceed? (yes/no): ").strip().lower()
    
    if first_confirm != "yes":
        print("❌ Operation cancelled.")
        return 0
    
    # Second confirmation for safety
    print("\n🚨 FINAL CONFIRMATION")
    print(f"🚨 You are about to delete {len(selected_buckets)} bucket(s) with {total_objects} objects")
    second_confirm = input("Type 'DELETE' in capital letters to confirm: ").strip()
    
    if second_confirm != "DELETE":
        print("❌ Operation cancelled. (You must type 'DELETE' exactly)")
        return 0
    
    # Remove selected buckets with progress
    print("\n" + "="*80)
    print("Starting deletion...")
    print("="*80 + "\n")
    
    removed_count = 0
    failed_count = 0
    
    for idx, bucket in enumerate(selected_buckets, start=1):
        print(f"[{idx}/{len(selected_buckets)}] Removing bucket '{bucket['name']}'...", end=" ")
        force = bucket['object_count'] > 0
        
        if remove_bucket(client, bucket["name"], force=force, quiet=True):
            print("✅")
            removed_count += 1
        else:
            print("❌")
            failed_count += 1
    
    # Final summary
    print("\n" + "="*80)
    print("DELETION SUMMARY")
    print("="*80)
    print(f"✅ Successfully removed: {removed_count} bucket(s)")
    if failed_count > 0:
        print(f"❌ Failed to remove: {failed_count} bucket(s)")
    print(f"📊 Total objects deleted: {total_objects}")
    print(f"💾 Total space freed: {total_size_mb:.2f} MB")
    print("="*80)
    
    return removed_count


# ============================================================================
# MAIN
# ============================================================================
def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="MinIO Bucket Cleanup Utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all buckets
  python cleanup_minio_buckets.py --list
  
  # Interactive mode - select buckets to remove
  python cleanup_minio_buckets.py --interactive
  python cleanup_minio_buckets.py -i
  
  # Show what would be deleted (dry run)
  python cleanup_minio_buckets.py --dry-run
  
  # Remove empty buckets
  python cleanup_minio_buckets.py --remove-empty
  
  # Remove specific bucket
  python cleanup_minio_buckets.py --remove test-bucket
  
  # Remove specific bucket with objects (force)
  python cleanup_minio_buckets.py --remove test-bucket --force
  
  # Remove all non-protected buckets (DANGEROUS!)
  python cleanup_minio_buckets.py --remove-all
        """
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all buckets with details"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting"
    )
    
    parser.add_argument(
        "--remove-empty",
        action="store_true",
        help="Remove all empty buckets"
    )
    
    parser.add_argument(
        "--remove",
        type=str,
        metavar="BUCKET",
        help="Remove specific bucket by name"
    )
    
    parser.add_argument(
        "--remove-all",
        action="store_true",
        help="Remove ALL non-protected buckets (DANGEROUS!)"
    )
    
    parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="Interactive mode to select buckets for removal"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force removal of non-empty buckets"
    )
    
    args = parser.parse_args()
    
    # Print header
    print("\n" + "="*80)
    print("MinIO Bucket Cleanup Utility")
    print("="*80)
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Protected buckets: {', '.join(PROTECTED_BUCKETS)}")
    print("="*80 + "\n")
    
    # Get MinIO client
    client = get_minio_client()
    
    # Get bucket list
    buckets = list_all_buckets(client)
    
    # Execute requested operation
    if args.interactive:
        interactive_remove_buckets(client, buckets)
    
    elif args.list or not any([args.dry_run, args.remove_empty, args.remove, args.remove_all]):
        print_bucket_list(buckets)
    
    elif args.dry_run:
        print_bucket_list(buckets)
        remove_empty_buckets(client, buckets, dry_run=True)
    
    elif args.remove_empty:
        remove_empty_buckets(client, buckets, dry_run=False)
    
    elif args.remove:
        remove_bucket(client, args.remove, force=args.force)
    
    elif args.remove_all:
        remove_all_buckets(client, buckets, dry_run=False)
    
    print("\n✅ Done!\n")


if __name__ == "__main__":
    main()
