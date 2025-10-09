#!/usr/bin/env python3
"""
Test script to validate the streaming pipeline structure without external dependencies
"""

import os
import sys
import json
from pathlib import Path

def test_pipeline_structure():
    """Test that all required files exist and have correct structure"""
    
    base_path = Path(__file__).parent
    
    # Test files exist
    required_files = [
        "ops/generate_data.py",
        "ops/load_data_to_psql.py", 
        "ops/__init__.py",
        "utils/fake_data_generator.py",
        "utils/psql_loader.py",
        "config/metadata.json",
        "pipeline/main.py",
        "README.md"
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = base_path / file_path
        if not full_path.exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    else:
        print("✅ All required files exist")
    
    # Test metadata.json structure
    metadata_path = base_path / "config" / "metadata.json"
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        required_tables = ["users", "transactions", "detailed_transactions"]
        for table in required_tables:
            if table not in metadata:
                print(f"❌ Table '{table}' missing from metadata.json")
                return False
            
            if "columns" not in metadata[table]:
                print(f"❌ Columns missing for table '{table}'")
                return False
        
        print("✅ metadata.json structure is valid")
        
    except Exception as e:
        print(f"❌ Error reading metadata.json: {e}")
        return False
    
    # Test Python syntax
    python_files = [
        "ops/generate_data.py",
        "ops/load_data_to_psql.py",
        "pipeline/main.py"
    ]
    
    for file_path in python_files:
        full_path = base_path / file_path
        try:
            with open(full_path, 'r') as f:
                code = f.read()
            compile(code, str(full_path), 'exec')
        except SyntaxError as e:
            print(f"❌ Syntax error in {file_path}: {e}")
            return False
    
    print("✅ All Python files have valid syntax")
    
    return True

def test_imports():
    """Test that imports can be resolved (basic check)"""
    
    # Test basic imports that should be available
    try:
        import os
        import sys
        import json
        import logging
        from pathlib import Path
        from datetime import datetime
        from typing import Dict, Any, List
        print("✅ Basic imports work")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Testing Streaming Pipeline Structure")
    print("=" * 50)
    
    structure_ok = test_pipeline_structure()
    imports_ok = test_imports()
    
    print("\n" + "=" * 50)
    if structure_ok and imports_ok:
        print("🎉 All tests passed! Pipeline structure is valid.")
        print("\n📋 Next steps:")
        print("1. Install dependencies: pip install faker pandas sqlalchemy psycopg2-binary python-dotenv")
        print("2. Configure PostgreSQL connection in .env file")
        print("3. Run the pipeline: python pipeline/main.py")
    else:
        print("❌ Some tests failed. Please fix the issues above.")
        sys.exit(1)

if __name__ == "__main__":
    main()