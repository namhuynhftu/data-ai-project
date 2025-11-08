#!/usr/bin/env python3
"""
Configuration Validation Script

Run this script on application startup to validate configuration
and fail fast if anything is misconfigured.

Usage:
    python -m elt_pipeline.streaming.common.validate_config
    
    # Or as part of Docker healthcheck
    docker run --entrypoint python myapp -m elt_pipeline.streaming.common.validate_config

Exit Codes:
    0 - Configuration valid
    1 - Configuration invalid (errors found)
    2 - Configuration valid with warnings

Best Practice: Run this as a pre-flight check before starting
producers/consumers to catch configuration errors early.
"""

import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from elt_pipeline.streaming.common.config import streaming_config
from elt_pipeline.streaming.common.logger import get_streaming_logger

logger = get_streaming_logger('config.validation')


def print_validation_report(result: dict) -> None:
    """
    Print human-readable validation report.
    
    Args:
        result: Validation result dictionary
    """
    print("\n" + "="*70)
    print("🔍 STREAMING PIPELINE CONFIGURATION VALIDATION")
    print("="*70)
    
    print(f"\n📊 Environment: {result['environment']}")
    print(f"⏰ Validated at: {result['validated_at']}")
    
    # Print check results
    print("\n✅ Configuration Checks:")
    for check_name, passed in result['checks'].items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status} - {check_name.replace('_', ' ').title()}")
    
    # Print errors
    if result['errors']:
        print(f"\n❌ Errors ({len(result['errors'])}):")
        for error in result['errors']:
            print(f"   ❌ {error}")
    
    # Print warnings
    if result['warnings']:
        print(f"\n⚠️  Warnings ({len(result['warnings'])}):")
        for warning in result['warnings']:
            print(f"   ⚠️  {warning}")
    
    # Overall status
    print("\n" + "="*70)
    if result['valid']:
        if result['warnings']:
            print("✅ Configuration is VALID (with warnings)")
        else:
            print("✅ Configuration is VALID")
    else:
        print("❌ Configuration is INVALID")
    print("="*70 + "\n")


def main() -> int:
    """
    Main validation function.
    
    Returns:
        Exit code (0=success, 1=errors, 2=warnings)
    """
    try:
        logger.info("Starting configuration validation...")
        
        # Validate configuration
        result = streaming_config.validate()
        
        # Print report
        print_validation_report(result)
        
        # Also log as JSON for parsing
        logger.info(f"Validation result: {json.dumps(result)}")
        
        # Determine exit code
        if not result['valid']:
            logger.error("Configuration validation FAILED")
            return 1
        elif result['warnings']:
            logger.warning("Configuration validation passed with warnings")
            return 2
        else:
            logger.info("Configuration validation PASSED")
            return 0
            
    except Exception as e:
        logger.error(f"Configuration validation crashed: {e}", exc_info=True)
        print(f"\n❌ VALIDATION CRASHED: {e}\n")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
