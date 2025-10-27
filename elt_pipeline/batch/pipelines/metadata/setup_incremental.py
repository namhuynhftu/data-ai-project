"""
Setup Incremental Load Metadata
This script resets the loaded_at.json file for incremental tables.
Sets load_from to null and load_at to 2018-01-01 00:00:00 for a fresh start.
"""
import json
from datetime import datetime
from pathlib import Path

# Path to the loaded_at.json file
LOADED_AT_FILE = Path(__file__).parent / "loaded_at.json"

# Initial load_at timestamp (start of data range)
INITIAL_LOAD_AT = "2018-01-01 00:00:00"

def reset_incremental_metadata():
    """
    Reset all incremental load tracking to initial state.
    Sets load_from = null and load_at = 2018-01-01 00:00:00 for all tables.
    """
    print("🔄 Incremental Load Metadata Setup")
    print("=" * 60)
    print(f"File: {LOADED_AT_FILE}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check if file exists
    if not LOADED_AT_FILE.exists():
        print("⚠️  Warning: loaded_at.json file not found!")
        print(f"   Expected location: {LOADED_AT_FILE}")
        return
    
    # Load current metadata
    try:
        with open(LOADED_AT_FILE, 'r') as f:
            metadata = json.load(f)
        
        print(f"\n✅ Loaded existing metadata")
        print(f"   Tables tracked: {len(metadata.get('tables', {}))}")
        
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON file: {e}")
        return
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Reset all tables
    print(f"\n🔄 Resetting incremental load tracking...")
    print("-" * 60)
    
    if "tables" not in metadata:
        print("⚠️  No tables found in metadata")
        return
    
    updated_count = 0
    
    for table_name, table_data in metadata["tables"].items():
        old_load_from = table_data.get("load_from")
        old_load_at = table_data.get("load_at")
        
        # Update to initial state
        table_data["load_from"] = None
        table_data["load_at"] = INITIAL_LOAD_AT
        
        print(f"   📊 {table_name}:")
        print(f"      load_from: {old_load_from} → null")
        print(f"      load_at:   {old_load_at} → {INITIAL_LOAD_AT}")
        
        updated_count += 1
    
    # Update last_updated timestamp
    metadata["last_updated"] = datetime.now().isoformat()
    
    # Write back to file
    try:
        with open(LOADED_AT_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"\n" + "=" * 60)
        print("✅ Metadata reset successfully!")
        print("=" * 60)
        print(f"📊 Summary:")
        print(f"   Tables updated: {updated_count}")
        print(f"   Initial load_at: {INITIAL_LOAD_AT}")
        print(f"   All load_from values set to: null")
        print("=" * 60)
        print("\n💡 Next steps:")
        print("   1. Run the pipeline with option 3 (incremental tables only)")
        print("   2. First run will load data from beginning to 2018-01-01")
        print("   3. Subsequent runs will load incrementally from last load_at")
        
    except Exception as e:
        print(f"❌ Error writing file: {e}")
        import traceback
        traceback.print_exc()


def show_current_state():
    """Display the current state of loaded_at.json"""
    print("📋 Current Incremental Load State")
    print("=" * 60)
    
    if not LOADED_AT_FILE.exists():
        print("⚠️  loaded_at.json file not found!")
        return
    
    try:
        with open(LOADED_AT_FILE, 'r') as f:
            metadata = json.load(f)
        
        print(f"Last Updated: {metadata.get('last_updated', 'N/A')}")
        print(f"\nTables tracked: {len(metadata.get('tables', {}))}\n")
        
        if "tables" in metadata:
            for table_name, table_data in metadata["tables"].items():
                print(f"📊 {table_name}:")
                print(f"   load_from: {table_data.get('load_from')}")
                print(f"   load_at:   {table_data.get('load_at')}")
                print(f"   watermark: {table_data.get('watermark_column')}")
                print()
        
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    import sys
    
    print("\n" + "=" * 60)
    print("🔄 INCREMENTAL LOAD METADATA SETUP")
    print("=" * 60)
    print("Options:")
    print("  1. Reset metadata (load_from=null, load_at=2018-01-01)")
    print("  2. Show current state")
    print("=" * 60)
    
    choice = input("\nEnter your choice (1-2): ").strip()
    
    if choice == "1":
        print("\n⚠️  This will reset all incremental load tracking!")
        confirmation = input("Are you sure? (yes/no): ").strip().lower()
        
        if confirmation == "yes":
            reset_incremental_metadata()
        else:
            print("\n❌ Operation cancelled")
    
    elif choice == "2":
        show_current_state()
    
    else:
        print("\n❌ Invalid choice")
        sys.exit(1)
