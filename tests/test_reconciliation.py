"""
Reconciliation Test Suite

This module tests reconciliation checks — comparing two datasets (source vs target)
to verify they match across record counts, aggregated sums, and row-level values.

Reconciliation answers: "Did all the data arrive correctly from source to target?"

Design Principle:
    Tests are data-agnostic and config-driven. No hard-coded counts, sums, or
    column names. All expected values are derived from the DataFrames themselves,
    and all parameters (column names, key columns) come from config.yaml.
    To adapt the framework, edit only config.yaml — no test code changes needed.

Fixtures (provided by conftest.py):
    - spark_session: Spark session for the test run
    - reconciliation_source_data: Source DataFrame (reconciliation_source.csv)
    - reconciliation_target_data: Target DataFrame matching source (reconciliation_target.csv)
    - reconciliation_mismatch_target_data: Target with intentional differences
    - reconciliation_config: Reconciliation rules loaded from config.yaml

Test Data:
    - reconciliation_source.csv: Source dataset (e.g., upstream orders)
    - reconciliation_target.csv: Identical copy of source (all checks should pass)
    - reconciliation_mismatch_target.csv: Target with intentional differences
      (missing rows, extra rows, value mismatches)
    - config.yaml: Reconciliation rules (record_count, sum, row_level)

Functions tested (from dq_checks.py):
    - check_record_count(source_df, target_df)
    - check_sum_reconciliation(source_df, target_df, column_name)
    - check_row_level_reconciliation(source_df, target_df, key_columns)
    - check_all_reconciliation(source_df, target_df, reconciliation_config)
    - get_mismatched_rows(source_df, target_df, key_columns)
"""

import sys
from pathlib import Path
from typing import List, Dict

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum


# ==============================================================================
# Path Configuration
# ==============================================================================

TEST_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = TEST_DIR.parent
CHECKS_DIR = PROJECT_ROOT / "src" / "checks"

# Add checks module to path
sys.path.insert(0, str(CHECKS_DIR))
from dq_checks import (
    check_record_count,
    check_sum_reconciliation,
    check_row_level_reconciliation,
    check_all_reconciliation,
    get_mismatched_rows,
)


# ==============================================================================
# Record Count Reconciliation Tests
# ==============================================================================

def test_record_count_match(reconciliation_source_data: DataFrame,
                              reconciliation_target_data: DataFrame):
    """Record count reconciliation passes when source and target have same row count"""
    source_count = reconciliation_source_data.count()
    target_count = reconciliation_target_data.count()
    
    result = check_record_count(reconciliation_source_data, reconciliation_target_data)
    
    # Verify function reads counts correctly from DataFrames
    assert result["check"] == "record_count_reconciliation"
    assert result["source_count"] == source_count
    assert result["target_count"] == target_count
    assert result["count_diff"] == source_count - target_count
    # Verify pass/fail logic: passed should be True when counts match
    assert result["passed"] == (source_count == target_count)
    # Test data integrity: source and target should have matching counts
    assert source_count == target_count
    assert result["passed"] == True


def test_record_count_mismatch(reconciliation_source_data: DataFrame,
                                 reconciliation_mismatch_target_data: DataFrame):
    """Record count reconciliation detects when source and target counts differ"""
    source_count = reconciliation_source_data.count()
    target_count = reconciliation_mismatch_target_data.count()
    
    result = check_record_count(reconciliation_source_data, reconciliation_mismatch_target_data)
    
    assert result["check"] == "record_count_reconciliation"
    assert result["source_count"] == source_count
    assert result["target_count"] == target_count
    assert result["count_diff"] == source_count - target_count
    # Verify pass/fail logic: passed should be False when counts differ
    assert result["passed"] == (source_count == target_count)
    # Test data integrity: mismatch data should have different counts
    assert source_count != target_count
    assert result["passed"] == False


# ==============================================================================
# Sum Reconciliation Tests
# ==============================================================================

def test_sum_reconciliation_passes(reconciliation_source_data: DataFrame,
                                     reconciliation_target_data: DataFrame,
                                     reconciliation_config: List[Dict]):
    """Sum reconciliation passes when column totals match between source and target"""
    column = next(r for r in reconciliation_config if r["check"] == "sum")["column"]
    
    # Independently compute expected sums from the DataFrames
    source_sum = reconciliation_source_data.agg(spark_sum(col(column))).collect()[0][0] or 0
    target_sum = reconciliation_target_data.agg(spark_sum(col(column))).collect()[0][0] or 0
    expected_diff = round(source_sum - target_sum, 2)
    
    result = check_sum_reconciliation(reconciliation_source_data, reconciliation_target_data, column)
    
    assert result["check"] == "sum_reconciliation"
    assert result["column"] == column
    assert result["source_sum"] == source_sum
    assert result["target_sum"] == target_sum
    assert result["diff"] == expected_diff
    # Verify pass/fail logic: passed when diff is within tolerance
    assert result["passed"] == (abs(expected_diff) <= 0.01)
    # Test data integrity: source and target should have matching sums
    assert source_sum == target_sum
    assert result["passed"] == True


def test_sum_reconciliation_detects_mismatch(reconciliation_source_data: DataFrame,
                                                reconciliation_mismatch_target_data: DataFrame,
                                                reconciliation_config: List[Dict]):
    """Sum reconciliation detects when a column total differs between source and target"""
    column = next(r for r in reconciliation_config if r["check"] == "sum")["column"]
    
    # Independently compute expected sums from the DataFrames
    source_sum = reconciliation_source_data.agg(spark_sum(col(column))).collect()[0][0] or 0
    target_sum = reconciliation_mismatch_target_data.agg(spark_sum(col(column))).collect()[0][0] or 0
    expected_diff = round(source_sum - target_sum, 2)
    
    result = check_sum_reconciliation(reconciliation_source_data, reconciliation_mismatch_target_data, column)
    
    assert result["check"] == "sum_reconciliation"
    assert result["source_sum"] == source_sum
    assert result["target_sum"] == target_sum
    assert result["diff"] == expected_diff
    # Verify pass/fail logic: passed when diff is within tolerance
    assert result["passed"] == (abs(expected_diff) <= 0.01)
    # Test data integrity: mismatch data should have different sums
    assert source_sum != target_sum
    assert result["passed"] == False


# ==============================================================================
# Row-Level Reconciliation Tests
# ==============================================================================

def test_row_level_reconciliation_passes(reconciliation_source_data: DataFrame,
                                            reconciliation_target_data: DataFrame,
                                            reconciliation_config: List[Dict]):
    """Row-level reconciliation passes when all rows match across source and target"""
    key_columns = next(r for r in reconciliation_config if r["check"] == "row_level")["key_columns"]
    
    result = check_row_level_reconciliation(reconciliation_source_data, reconciliation_target_data, key_columns)
    
    assert result["check"] == "row_level_reconciliation"
    assert result["key_columns"] == key_columns
    assert result["source_count"] == reconciliation_source_data.count()
    assert result["target_count"] == reconciliation_target_data.count()
    # No differences should exist when source and target are identical
    assert result["missing_count"] == 0
    assert result["extra_count"] == 0
    assert result["mismatch_count"] == 0
    assert result["passed"] == True


def test_row_level_finds_missing_rows(reconciliation_source_data: DataFrame,
                                         reconciliation_mismatch_target_data: DataFrame,
                                         reconciliation_config: List[Dict]):
    """Row-level reconciliation detects rows in source but missing from target"""
    key_columns = next(r for r in reconciliation_config if r["check"] == "row_level")["key_columns"]
    
    result = check_row_level_reconciliation(reconciliation_source_data, reconciliation_mismatch_target_data, key_columns)
    
    # Verify missing rows are detected (rows in source whose key is not in target)
    assert result["missing_count"] > 0, "Should detect rows in source missing from target"
    assert result["passed"] == False


def test_row_level_finds_extra_rows(reconciliation_source_data: DataFrame,
                                      reconciliation_mismatch_target_data: DataFrame,
                                      reconciliation_config: List[Dict]):
    """Row-level reconciliation detects rows in target but missing from source"""
    key_columns = next(r for r in reconciliation_config if r["check"] == "row_level")["key_columns"]
    
    result = check_row_level_reconciliation(reconciliation_source_data, reconciliation_mismatch_target_data, key_columns)
    
    # Verify extra rows are detected (rows in target whose key is not in source)
    assert result["extra_count"] > 0, "Should detect rows in target missing from source"
    assert result["passed"] == False


def test_get_mismatched_rows(reconciliation_source_data: DataFrame,
                                reconciliation_mismatch_target_data: DataFrame,
                                reconciliation_config: List[Dict]):
    """get_mismatched_rows returns the actual rows that differ between source and target"""
    key_columns = next(r for r in reconciliation_config if r["check"] == "row_level")["key_columns"]
    
    # Get the summary from check_row_level_reconciliation
    check_result = check_row_level_reconciliation(reconciliation_source_data, reconciliation_mismatch_target_data, key_columns)
    
    # Get the actual mismatched rows from source
    mismatched = get_mismatched_rows(reconciliation_source_data, reconciliation_mismatch_target_data, key_columns)
    actual_count = mismatched.count()
    
    # get_mismatched_rows returns rows from source that are missing or have value mismatches
    # This should equal missing_count + mismatch_count from the check function
    expected_count = check_result["missing_count"] + check_result["mismatch_count"]
    assert actual_count == expected_count
    assert actual_count > 0, "Mismatched data should produce at least one mismatched row"


# ==============================================================================
# Batch Reconciliation Tests
# ==============================================================================

def test_check_all_reconciliation_from_yaml_config(reconciliation_source_data: DataFrame,
                                                     reconciliation_target_data: DataFrame,
                                                     reconciliation_config: List[Dict]):
    """Batch reconciliation runs all checks from YAML config and all pass on matching data"""
    results = check_all_reconciliation(reconciliation_source_data, reconciliation_target_data, reconciliation_config)
    
    # Should return one result per config entry
    assert len(results) == len(reconciliation_config)
    # All checks should pass when source and target are identical
    for r in results:
        assert r["passed"] == True, f"Check '{r['check']}' should pass on matching data"


def test_check_all_reconciliation_detects_failures(reconciliation_source_data: DataFrame,
                                                      reconciliation_mismatch_target_data: DataFrame,
                                                      reconciliation_config: List[Dict]):
    """Batch reconciliation detects failures when source and target don't match"""
    results = check_all_reconciliation(reconciliation_source_data, reconciliation_mismatch_target_data, reconciliation_config)
    
    assert len(results) == len(reconciliation_config)
    # At least one check should fail on mismatched data
    failed_checks = [r for r in results if not r["passed"]]
    assert len(failed_checks) > 0, "At least one check should fail on mismatched data"
    # Row-level check should always fail when rows differ
    row_level_result = next(r for r in results if r["check"] == "row_level_reconciliation")
    assert row_level_result["passed"] == False


# ==============================================================================
# Test Runner
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, '-v', '-s'])
