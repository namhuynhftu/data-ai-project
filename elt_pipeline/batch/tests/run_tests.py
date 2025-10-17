"""
Test runner for batch pipeline operations.
Runs all unit tests for the ops modules with coverage reporting.
"""
import pytest
import sys
import os
from pathlib import Path


def main():
    """Run all tests with coverage and reporting."""
    
    # Get the test directory
    test_dir = Path(__file__).parent
    ops_dir = test_dir.parent / "ops"
    
    print("🧪 Running Batch Pipeline Operations Tests")
    print("=" * 60)
    print(f"Test directory: {test_dir}")
    print(f"Ops directory: {ops_dir}")
    print("-" * 60)
    
    # Test configuration
    pytest_args = [
        str(test_dir),  # Test directory
        "-v",           # Verbose output
        "--tb=short",   # Short traceback format
        "--strict-markers",  # Strict marker handling
        "-ra",          # Show all test results
        f"--cov={ops_dir}",  # Coverage for ops directory
        "--cov-report=term-missing",  # Show missing lines
        "--cov-report=html:htmlcov",  # HTML coverage report
        "--cov-fail-under=80",  # Fail if coverage below 80%
    ]
    
    # Add test files explicitly
    test_files = [
        "test_extract_data_from_mysql.py",
        "test_load_data_to_minio.py", 
        "test_load_data_to_snowflake.py"
    ]
    
    print("📋 Test files to execute:")
    for test_file in test_files:
        test_path = test_dir / test_file
        if test_path.exists():
            print(f"   ✅ {test_file}")
        else:
            print(f"   ❌ {test_file} (missing)")
    
    print("-" * 60)
    
    # Run pytest
    try:
        exit_code = pytest.main(pytest_args)
        
        print("\n" + "=" * 60)
        if exit_code == 0:
            print("✅ All tests passed!")
            print("📊 Coverage report generated in htmlcov/index.html")
        else:
            print("❌ Some tests failed or coverage is below threshold")
            print("📊 Check the output above for details")
        
        print("=" * 60)
        return exit_code
        
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return 1


def run_specific_test(test_name):
    """Run a specific test file."""
    test_dir = Path(__file__).parent
    test_file = test_dir / f"test_{test_name}.py"
    
    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        return 1
    
    print(f"🧪 Running specific test: {test_name}")
    print("-" * 40)
    
    pytest_args = [
        str(test_file),
        "-v",
        "--tb=short"
    ]
    
    return pytest.main(pytest_args)


def show_coverage():
    """Show coverage report."""
    try:
        import coverage
        
        cov = coverage.Coverage()
        cov.load()
        cov.report()
        
    except ImportError:
        print("❌ Coverage module not installed")
        print("Install with: pip install coverage")
        return 1
    except Exception as e:
        print(f"❌ Error showing coverage: {e}")
        return 1


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "coverage":
            exit_code = show_coverage()
        elif command.startswith("test_"):
            # Remove "test_" prefix if provided
            test_name = command[5:] if command.startswith("test_") else command
            exit_code = run_specific_test(test_name)
        elif command in ["mysql", "minio", "snowflake"]:
            # Map service names to test names
            service_map = {
                "mysql": "extract_data_from_mysql",
                "minio": "load_data_to_minio", 
                "snowflake": "load_data_to_snowflake"
            }
            exit_code = run_specific_test(service_map[command])
        elif command == "--help" or command == "-h":
            print("🧪 Batch Pipeline Test Runner")
            print("-" * 30)
            print("Usage:")
            print("  python run_tests.py              # Run all tests")
            print("  python run_tests.py mysql        # Test MySQL ops")
            print("  python run_tests.py minio        # Test MinIO ops")
            print("  python run_tests.py snowflake    # Test Snowflake ops")
            print("  python run_tests.py coverage     # Show coverage report")
            print("  python run_tests.py --help       # Show this help")
            exit_code = 0
        else:
            print(f"❌ Unknown command: {command}")
            print("Use --help for available commands")
            exit_code = 1
    else:
        # Run all tests
        exit_code = main()
    
    sys.exit(exit_code)