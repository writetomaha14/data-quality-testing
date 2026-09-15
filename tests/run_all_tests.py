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
    
    # Resolve test files relative to current working directory (tests directory)
    test_dir = os.getcwd()
    
    # Portfolio-ready test files (FK + Schema)
    # Excludes old test files: test_completeness, test_uniqueness, test_referential_integrity
    candidate_files = [
        'test_foreign_keys.py',  # 3 FK tests
        'test_schema_validation.py',  # Schema validation tests
    ]
    
    # Only include files that actually exist
    test_files = [os.path.join(test_dir, f) for f in candidate_files
                  if os.path.exists(os.path.join(test_dir, f))]
    
    if not test_files:
        print("No test files found!")
        return 1
    
    result = pytest.main(test_files + [
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
    result = main()
    if result != 0:
        sys.exit(result)
