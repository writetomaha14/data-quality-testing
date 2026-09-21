"""
Business Rules Test Suite

This module tests business rule validation using the same consistency checking
functions from dq_checks module. Business rules are domain-specific constraints
(allowed values, range limits, business logic) enforced via SQL expressions,
reusing check_consistency / check_all_consistency / get_violating_rows.

Fixtures:
    All fixtures (spark_session, business_rules, business_test_data,
    business_violation_data) are provided by conftest.py and automatically
    available. No need to import them.

Test Coverage (Implemented):
- Validation when all rows pass business rules (single rule + batch)
- Enum / allowed-values rule (valid_status_enum)
- Business logic constraint rule (discount_within_total)
- Batch checking with multiple rules from YAML config
- Detection of rows that violate business rules
- Helper function get_violating_rows() for retrieving violating rows

Test Coverage (Pending):
- Error handling for invalid SQL expressions

Test Data:
- orders_business_valid.csv: Orders that pass all business rules
  (status in allowed set, discount <= total)
- orders_business_violations.csv: Orders with intentional violations
  (2 rows violate valid_status_enum, 2 rows violate discount_within_total)
- config.yaml: Business rules loaded from YAML configuration
  (valid_status_enum, discount_within_total)
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

def test_business_single_rule_passes(business_test_data: DataFrame,
                                       business_rules: List[Dict]):
    """Verify check passes when all rows satisfy valid_status_enum rule"""
    orders = business_test_data
    
    # Load the valid_status_enum rule from YAML config
    status_rule = next(r for r in business_rules if r["name"] == "valid_status_enum")
    
    print(f"\n🔍 Running business rule test with {orders.count()} valid orders")
    print(f"📋 Loaded rule from YAML: {status_rule['name']} -> {status_rule['expression']}")
    
    result = check_consistency(orders, status_rule["name"], status_rule["expression"])
    print(f"📊 Business rule check result: {result}")
    
    # Assertions for passing scenario
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["rule_name"] == "valid_status_enum", "Rule name should match YAML config"
    assert result["violation_count"] == 0, "All rows should pass the rule"
    assert result["passed"] == True, "Check should pass when no violations exist"
    assert result["total_rows"] == orders.count(), "Total rows should match DataFrame count"


def test_business_discount_rule_passes(business_test_data: DataFrame,
                                          business_rules: List[Dict]):
    """Verify discount_within_total rule passes when discount <= total"""
    orders = business_test_data
    
    # Load the discount_within_total rule from YAML config
    discount_rule = next(r for r in business_rules if r["name"] == "discount_within_total")
    
    print(f"\n🔍 Testing discount rule: {discount_rule['expression']}")
    result = check_consistency(orders, discount_rule["name"], discount_rule["expression"])
    print(f"📊 Discount rule check result: {result}")
    
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["rule_name"] == "discount_within_total", "Rule name should match YAML config"
    assert result["violation_count"] == 0, "All rows should have discount <= total"
    assert result["passed"] == True, "Check should pass when all discounts are within total"


def test_check_all_business_rules_from_yaml_config(business_test_data: DataFrame,
                                                     business_rules: List[Dict]):
    """Verify batch checking with all business rules loaded from YAML config"""
    orders = business_test_data
    
    print(f"\n🔍 Running all {len(business_rules)} business rules from YAML config")
    results = check_all_consistency(orders, business_rules)
    
    for r in results:
        print(f"📊 {r['rule_name']}: violations={r['violation_count']}, passed={r['passed']}")
    
    assert len(results) == len(business_rules), "Should return one result per rule"
    for r in results:
        assert r["passed"] == True, f"Rule '{r['rule_name']}' should pass on valid data"
        assert r["violation_count"] == 0, f"Rule '{r['rule_name']}' should have no violations"


# ==============================================================================
# Failure Detection Tests
# ==============================================================================

def test_business_finds_status_violations(business_violation_data: DataFrame,
                                            business_rules: List[Dict]):
    """Verify check detects rows where status is not in allowed values"""
    orders = business_violation_data
    
    status_rule = next(r for r in business_rules if r["name"] == "valid_status_enum")
    
    print(f"\n🔍 Running violation test with {orders.count()} orders (some with invalid status)")
    result = check_consistency(orders, status_rule["name"], status_rule["expression"])
    print(f"📊 Business rule check result: {result}")
    
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["violation_count"] == 2, "Should find 2 rows with invalid status"
    assert result["passed"] == False, "Check should fail when violations exist"
    assert result["total_rows"] == orders.count(), "Total rows should match DataFrame count"


def test_business_finds_discount_violations(business_violation_data: DataFrame,
                                               business_rules: List[Dict]):
    """Verify check detects rows where discount > total"""
    orders = business_violation_data
    
    discount_rule = next(r for r in business_rules if r["name"] == "discount_within_total")
    
    print(f"\n🔍 Running violation test with {orders.count()} orders (some with discount > total)")
    result = check_consistency(orders, discount_rule["name"], discount_rule["expression"])
    print(f"📊 Discount rule check result: {result}")
    
    assert result["check"] == "consistency", "Check type should be 'consistency'"
    assert result["violation_count"] == 2, "Should find 2 rows with discount > total"
    assert result["passed"] == False, "Check should fail when violations exist"


def test_get_violating_rows_for_business_rules(business_violation_data: DataFrame,
                                                  business_rules: List[Dict]):
    """Verify get_violating_rows returns rows matching the violation count"""
    orders = business_violation_data
    
    status_rule = next(r for r in business_rules if r["name"] == "valid_status_enum")
    
    print(f"\n🔍 Testing get_violating_rows function for business rules")
    result = check_consistency(orders, status_rule["name"], status_rule["expression"])
    violating = get_violating_rows(orders, status_rule["expression"])
    
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