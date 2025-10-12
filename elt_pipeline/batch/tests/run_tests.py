#!/usr/bin/env python3
"""
Comprehensive test runner for the batch pipeline.

This script runs all tests and provides a summary of test coverage
and results for the entire batch pipeline system.
"""

import pytest
import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def run_all_tests():
    """Run all tests for the batch pipeline with coverage reporting."""
    
    test_directory = Path(__file__).parent
    
    # Define test modules to run
    test_modules = [
        "test_mysql_loader.py",
        "test_minio_loader.py", 
        "test_snowflake_loader.py",
        "test_operations_integration.py"
    ]
    
    print("🚀 Running Batch Pipeline Test Suite")
    print("=" * 50)
    
    # Run each test module
    all_passed = True
    results = {}
    
    for test_module in test_modules:
        test_path = test_directory / test_module
        
        if not test_path.exists():
            print(f"⚠️  Test module {test_module} not found, skipping...")
            continue
            
        print(f"\n📋 Running {test_module}...")
        print("-" * 30)
        
        # Run pytest for this module
        result = pytest.main([
            str(test_path),
            "-v",
            "--tb=short",
            "--no-header"
        ])
        
        results[test_module] = result
        if result != 0:
            all_passed = False
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 TEST SUITE SUMMARY")
    print("=" * 50)
    
    for test_module, result in results.items():
        status = "✅ PASSED" if result == 0 else "❌ FAILED"
        print(f"{test_module:<35} {status}")
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 ALL TESTS PASSED! Your batch pipeline is ready for production.")
    else:
        print("⚠️  SOME TESTS FAILED. Please review the output above.")
    print("=" * 50)
    
    return 0 if all_passed else 1


def run_coverage_tests():
    """Run tests with coverage reporting."""
    
    test_directory = Path(__file__).parent
    
    print("🚀 Running Batch Pipeline Tests with Coverage")
    print("=" * 60)
    
    # Run pytest with coverage
    result = pytest.main([
        str(test_directory),
        "-v",
        "--cov=elt_pipeline.batch",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        "--tb=short"
    ])
    
    if result == 0:
        print("\n🎉 All tests passed with coverage report generated!")
        print("📈 Coverage report available in 'htmlcov/index.html'")
    else:
        print("\n⚠️  Some tests failed. Please review the output above.")
    
    return result


def run_specific_test(test_name):
    """Run a specific test module or test function."""
    
    test_directory = Path(__file__).parent
    
    if test_name.endswith('.py'):
        test_path = test_directory / test_name
    else:
        # Search for test function across all modules
        test_path = str(test_directory)
        test_name = f"-k {test_name}"
    
    print(f"🎯 Running specific test: {test_name}")
    print("=" * 50)
    
    if isinstance(test_path, Path):
        result = pytest.main([str(test_path), "-v"])
    else:
        result = pytest.main([test_path, test_name, "-v"])
    
    return result


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "coverage":
            exit_code = run_coverage_tests()
        elif command == "specific" and len(sys.argv) > 2:
            test_name = sys.argv[2]
            exit_code = run_specific_test(test_name)
        else:
            print("Usage:")
            print("  python run_tests.py                 # Run all tests")
            print("  python run_tests.py coverage        # Run tests with coverage")
            print("  python run_tests.py specific <test> # Run specific test")
            exit_code = 1
    else:
        exit_code = run_all_tests()
    
    sys.exit(exit_code)