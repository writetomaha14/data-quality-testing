"""
# Uniqueness Tests

This module contains automated tests for data quality uniqueness checks. It validates that the `dq_checks` module
correctly identifies duplicate values in single columns and composite keys. Each test function is self-contained,
loads fresh test data, and verifies specific behavior of the uniqueness checking functions. The test suite runs
automatically when the file is executed, providing detailed output for each test including pass/fail status and
the actual results returned by the functions being tested.
"""

import sys
import logging
sys.path.append('/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/src/checks')
from dq_checks import get_duplicate_rows, check_uniqueness_composite, check_uniqueness



test_data_path = "/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/tests/test_data/employees.csv"

def load_test_df():
    """Load the test CSV file as a Spark DataFrame."""
    return spark.read.option("header", True).option("inferSchema", True).csv(test_data_path)

def test_uniqueness_id_has_duplicate():
    """
    Validates that check_uniqueness() correctly identifies when a column contains duplicate values.
    
    This test verifies the function returns:
    - The correct count of duplicates found
    - passed=False when duplicates exist (indicating the uniqueness check failed)
    """
    df = load_test_df()
    result = check_uniqueness(df, "id")
    print(f"\n✓ Test: ID uniqueness check")
    print(f"  Result: {result}")
    assert result["duplicate_count"] == 1, f"Expected 1 duplicate, got {result['duplicate_count']}"
    assert result["passed"] == False, "Check should fail when duplicates exist"
    print("  PASSED ✓")

def test_uniqueness_name_no_department_no_duplicates_check():
    """
    Validates how check_uniqueness() handles columns with null values and duplicates.
    
    This test observes the function's behavior when checking uniqueness on a column
    that may contain both missing values (nulls) and repeated non-null values.
    Currently prints results without assertions to understand the behavior.
    """
    df = load_test_df()
    result = check_uniqueness(df, "department")
    print(f"\n✓ Test: Department uniqueness check")
    print(f"  Result: {result}")
    print("  (This test just prints - no assertions yet)")

def test_get_duplicate_rows_returns_only_extras():
    """
    Validates that get_duplicate_rows() returns only the extra occurrences, not the first occurrence.
    
    When a value appears multiple times, this function should return N-1 rows (excluding the original).
    For example: if value 'X' appears 3 times, it should return 2 duplicate rows.
    """
    df = load_test_df()
    dup_rows = get_duplicate_rows(df, "id")
    print(f"\n✓ Test: Get duplicate rows")
    print(f"  Duplicate count: {dup_rows.count()}")
    assert dup_rows.count() == 1, f"Expected 1 duplicate row, got {dup_rows.count()}"
    print("  PASSED ✓")

def test_uniqueness_composite_id_and_name():
    """
    Validates uniqueness check across multiple columns (composite key).
    
    This test verifies that check_uniqueness_composite() correctly identifies duplicates
    when considering the combination of multiple columns together, rather than individually.
    Useful when individual columns can repeat, but their combination should be unique.
    """
    df = load_test_df()
    result = check_uniqueness_composite(df, ["id", "name"])
    print(f"\n✓ Test: Composite key uniqueness (id + name)")
    print(f"  Result: {result}")
    assert result["duplicate_count"] == 1, f"Expected 1 duplicate, got {result['duplicate_count']}"
    print("  PASSED ✓")


# Run all tests when file is executed
if __name__ == "__main__":
    print("="*60)
    print("RUNNING UNIQUENESS TESTS")
    print("="*60)
    
    try:
        test_uniqueness_id_has_duplicate()
    except AssertionError as e:
        print(f"  FAILED ✗: {e}")
    
    try:
        test_uniqueness_name_no_department_no_duplicates_check()
    except AssertionError as e:
        print(f"  FAILED ✗: {e}")
    
    try:
        test_get_duplicate_rows_returns_only_extras()
    except AssertionError as e:
        print(f"  FAILED ✗: {e}")
    
    try:
        test_uniqueness_composite_id_and_name()
    except AssertionError as e:
        print(f"  FAILED ✗: {e}")
    
    print("\n" + "="*60)
    print("ALL TESTS COMPLETED")
    print("="*60)