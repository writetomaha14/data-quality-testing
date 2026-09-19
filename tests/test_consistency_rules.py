"""
Consistency Rules Test Suite

This module tests the consistency checking functions from dq_checks module.
Verifies that business rule violations (within a single table) are correctly 
detected and reported.

Fixtures:
    All fixtures (spark_session, consistency_rules, consistency_test_data) are
    provided by conftest.py and automatically available. No need to import them.

Test Coverage (Implemented):
- Validation when all rows pass rules (single rule + batch)
- Date comparison rule (ship_after_order)
- Numeric calculation rule (total_matches_calculation)
- Batch checking with multiple rules from YAML config
- Detection of rows that violate consistency rules
- Helper function get_violating_rows() for retrieving violating rows

Test Coverage (Pending):
- Error handling for invalid SQL expressions

Test Data:
- orders_consistency_valid.csv: Orders that pass all consistency rules
  (ship_date >= order_date, total = quantity * unit_price)
- orders_consistency_violations.csv: Orders with intentional violations
  (2 rows violate ship_after_order, 2 rows violate total_matches_calculation)
- config.yaml: Consistency rules loaded from YAML configuration
  (ship_after_order, total_matches_calculation)
"""

import sys
from pathlib import Path
from typing import List, Dict

import pytest
from pyspark.sql import DataFrame


# ==============================================================================
# Path Configuration
# ==============================================================================

TEST_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = TEST_DIR.parent
CHECKS_DIR = PROJECT_ROOT / "src" / "checks"

# Add checks module to path
sys.path.insert(0, str(CHECKS_DIR))
from dq_checks import check_consistency, check_all_consistency, get_violating_rows


# ==============================================================================
# Test Cases
# ==============================================================================

def test_consistency_single_rule_passes(consistency_test_data: DataFrame,
                                         consistency_rules: List[Dict]):
    """Verify check passes when all rows satisfy a rule loaded from YAML config"""
    orders = consistency_test_data
    
    # Load the ship_after_order rule from YAML config
    ship_rule = next(r for r in consistency_rules if r["name"] == "ship_after_order")
    
    print(f"\n🔍 Running consistency test with {orders.count()} valid orders")
    print(f"📋 Loaded rule from YAML: {ship_rule['name']} -> {ship_rule['expression']}")
    
    result = check_consistency(orders, ship_rule["name"], ship_rule["expression"])
    print(f"📊 Consistency check result: {result}")
    
    # Assertions for passing scenario
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["rule_name"] == "ship_after_order", "Rule name should match YAML config"
    assert result["violation_count"] == 0, "All rows should pass the rule"
    assert result["passed"] == True, "Check should pass when no violations exist"
    assert result["total_rows"] == orders.count(), "Total rows should match DataFrame count"


def test_consistency_total_calculation_rule_passes(consistency_test_data: DataFrame,
                                                    consistency_rules: List[Dict]):
    """Verify total_matches_calculation rule passes when total = quantity * unit_price"""
    orders = consistency_test_data
    
    # Load the total_matches_calculation rule from YAML config
    total_rule = next(r for r in consistency_rules if r["name"] == "total_matches_calculation")
    
    print(f"\n🔍 Testing total calculation rule: {total_rule['expression']}")
    result = check_consistency(orders, total_rule["name"], total_rule["expression"])
    print(f"📊 Total calculation check result: {result}")
    
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["rule_name"] == "total_matches_calculation", "Rule name should match YAML config"
    assert result["violation_count"] == 0, "All rows should have matching totals"
    assert result["passed"] == True, "Check should pass when all totals match"


def test_check_all_consistency_from_yaml_config(consistency_test_data: DataFrame,
                                                 consistency_rules: List[Dict]):
    """Verify batch checking with all rules loaded from YAML config"""
    orders = consistency_test_data
    
    print(f"\n🔍 Running all {len(consistency_rules)} consistency rules from YAML config")
    results = check_all_consistency(orders, consistency_rules)
    
    for r in results:
        print(f"📊 {r['rule_name']}: violations={r['violation_count']}, passed={r['passed']}")
    
    assert len(results) == len(consistency_rules), "Should return one result per rule"
    for r in results:
        assert r["passed"] == True, f"Rule '{r['rule_name']}' should pass on valid data"
        assert r["violation_count"] == 0, f"Rule '{r['rule_name']}' should have no violations"


# ==============================================================================
# Failure Detection Tests
# ==============================================================================

def test_consistency_finds_ship_date_violations(consistency_violation_data: DataFrame,
                                                 consistency_rules: List[Dict]):
    """Verify check detects rows where ship_date < order_date"""
    orders = consistency_violation_data
    
    ship_rule = next(r for r in consistency_rules if r["name"] == "ship_after_order")
    
    print(f"\n🔍 Running violation test with {orders.count()} orders (some with bad ship dates)")
    result = check_consistency(orders, ship_rule["name"], ship_rule["expression"])
    print(f"📊 Consistency check result: {result}")
    
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["violation_count"] == 2, "Should find 2 rows with ship_date < order_date"
    assert result["passed"] == False, "Check should fail when violations exist"
    assert result["total_rows"] == orders.count(), "Total rows should match DataFrame count"


def test_consistency_finds_total_calculation_violations(consistency_violation_data: DataFrame,
                                                         consistency_rules: List[Dict]):
    """Verify check detects rows where total != quantity * unit_price"""
    orders = consistency_violation_data
    
    total_rule = next(r for r in consistency_rules if r["name"] == "total_matches_calculation")
    
    print(f"\n🔍 Running violation test with {orders.count()} orders (some with bad totals)")
    result = check_consistency(orders, total_rule["name"], total_rule["expression"])
    print(f"📊 Total calculation check result: {result}")
    
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["violation_count"] == 2, "Should find 2 rows with mismatched totals"
    assert result["passed"] == False, "Check should fail when violations exist"


def test_get_violating_rows_returns_correct_rows(consistency_violation_data: DataFrame,
                                                   consistency_rules: List[Dict]):
    """Verify get_violating_rows returns rows matching the violation count"""
    orders = consistency_violation_data
    
    ship_rule = next(r for r in consistency_rules if r["name"] == "ship_after_order")
    
    print(f"\n🔍 Testing get_violating_rows function")
    result = check_consistency(orders, ship_rule["name"], ship_rule["expression"])
    violating = get_violating_rows(orders, ship_rule["expression"])
    
    print(f"📊 Check function found: {result['violation_count']} violations")
    print(f"📊 Get function returned: {violating.count()} violating rows")
    
    assert violating.count() == result["violation_count"], "Violation count should match between check and get functions"
    assert violating.count() > 0, "Test data should contain violating records"


# ==============================================================================
# Test Runner
# ==============================================================================

if __name__ == "__main__":
    # Run pytest with verbose output and show logging
    # -v = verbose (show test names)
    # -s = show stdout/stderr (print statements and logging)
    # --log-cli-level=INFO = show INFO level logs in console
    pytest.main([__file__, '-v', '-s', '--log-cli-level=INFO'])
