"""
Test Runner Script

Run this before committing to ensure all tests pass.
This script executes all test files in the tests directory.

Usage:
    python run_all_tests.py
"""

import os
import sys
import pytest


def main():
    """Run all tests with minimal output."""
    print("=" * 70)
    print("Running All Data Quality Tests")
    print("=" * 70)
    
    # Disable bytecode writing (prevents __pycache__ issues in Databricks Repos)
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
    sys.dont_write_bytecode = True
    
    # Run all tests in current directory
    result = pytest.main([
        '.',  # Current directory (tests/)
        '-v',  # Verbose
        '-p', 'no:cacheprovider',  # Disable cache plugin
        '--tb=short',  # Short traceback format
        '-x',  # Stop on first failure
    ])
    
    print("\n" + "=" * 70)
    if result == 0:
        print("✅ ALL TESTS PASSED - Safe to commit!")
        print("=" * 70)
    else:
        print("❌ TESTS FAILED - Fix before adding new tests")
        print("=" * 70)
    
    return result


if __name__ == "__main__":
    sys.exit(main())
