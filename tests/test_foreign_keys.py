
"""
Test Suite for Foreign Key Integrity Checks

Tests for validating referential integrity between tables using PySpark DataFrames.
Covers both simple and composite foreign key relationships.

Fixtures:
    All fixtures (spark_session, test_data, fk_config, table_registry) are provided
    by conftest.py and automatically available. No need to import them.

Tests:
    - test_orders_to_customers_fk: Simple FK relationship
    - test_line_items_to_products_composite_fk: Composite FK relationship
    - test_check_all_foreign_keys_integration: Integration test for all FKs

Usage:
    pytest test_foreign_keys.py -v              # Run all FK tests
    pytest test_foreign_keys.py::test_orders_to_customers_fk -v  # Run specific test
    pytest test_foreign_keys.py -v -s           # Run with detailed output
"""

import sys
from pathlib import Path
from typing import Dict

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
from dq_checks import (
    check_referential_integrity,
    check_all_foreign_keys,
    check_referential_integrity_composite
)


# ==============================================================================
# Test Functions - Isolated, focused tests
# ==============================================================================

def test_orders_to_customers_fk(table_registry: Dict[str, DataFrame], 
                                 fk_config: Dict):
    """
    Test simple foreign key: orders.customer_id -> customers.customer_id
    
    This test validates that orders referencing non-existent customers are
    correctly identified as orphaned records.
    
    Expected Behavior (based on test data):
        - orders_fk.csv contains 1 order with invalid customer_id
        - The check should identify exactly 1 orphan
    """
    print("Testing orders -> customers FK relationship")
    
    # Get the specific FK config for this relationship
    orders_fk_config = [
        fk for fk in fk_config["foreign_keys"]
        if fk.get("child_table") == "orders" 
        and fk.get("parent_table") == "customers"
    ]
    
    assert len(orders_fk_config) == 1, "Expected exactly one orders->customers FK config"
    
    # Run the FK check
    result = check_referential_integrity(
        child_df=table_registry["orders"],
        parent_df=table_registry["customers"],
        child_key="customer_id",
        parent_key="customer_id"
    )
    
    # Validate results
    orphan_count = result["orphan_count"]
    print(f"  Found {orphan_count} orphaned orders")
    
    # Business expectation: Exactly 1 orphaned order in test data
    assert orphan_count == 1, (
        f"Expected 1 orphaned order (invalid customer_id), found {orphan_count}. "
        "Check orders_fk.csv test data."
    )
    assert not result["passed"], "FK check should fail when orphans exist"


def test_line_items_to_products_composite_fk(table_registry: Dict[str, DataFrame],
                                               fk_config: Dict):
    """
    Test composite foreign key: 
    order_line_items.(order_id, product_id) -> order_products.(order_id, product_id)
    
    This test validates that line items referencing non-existent order+product
    combinations are correctly identified as orphaned records.
    
    Expected Behavior (based on test data):
        - order_line_items.csv contains 1 line item with invalid order_id+product_id
        - The check should identify exactly 1 orphan
    """
    print("Testing order_line_items -> order_products composite FK relationship")
    
    # Get the specific FK config for this relationship
    composite_fk_config = [
        fk for fk in fk_config["foreign_keys"]
        if fk.get("child_table") == "order_line_items"
        and fk.get("parent_table") == "order_products"
    ]
    
    assert len(composite_fk_config) == 1, (
        "Expected exactly one order_line_items->order_products FK config"
    )
    
    # Run the composite FK check
    result = check_referential_integrity_composite(
        child_df=table_registry["order_line_items"],
        parent_df=table_registry["order_products"],
        child_keys=["order_id", "product_id"],
        parent_keys=["order_id", "product_id"]
    )
    
    # Validate results
    orphan_count = result["orphan_count"]
    print(f"  Found {orphan_count} orphaned line items")
    
    # Business expectation: Exactly 1 orphaned line item in test data
    assert orphan_count == 1, (
        f"Expected 1 orphaned line item (invalid order_id+product_id), found {orphan_count}. "
        "Check order_line_items.csv test data."
    )
    assert not result["passed"], "FK check should fail when orphans exist"



def test_check_all_foreign_keys_integration(table_registry: Dict[str, DataFrame],
                                             fk_config: Dict):
    """
    Integration test: Run all FK checks from configuration.
    
    This test validates that the check_all_foreign_keys function correctly
    processes multiple FK relationships (both simple and composite) from config.
    
    Expected Behavior:
        - All FK relationships in config.yaml are checked
        - Each check returns valid results
        - Orphan counts match individual test expectations
    """
    print("Running integration test for all FK relationships")
    
    # Run all FK checks from config
    results = check_all_foreign_keys(table_registry, fk_config["foreign_keys"])
    
    # Validate overall structure
    assert len(results) == 2, (
        f"Expected 2 FK relationships from config, got {len(results)}"
    )
    
    # Validate individual results
    for i, result in enumerate(results, 1):
        print(f"\n  Check {i}:")
        print(f"    Relationship: {result.get('child_table')} -> {result.get('parent_table')}")
        print(f"    Orphan count: {result['orphan_count']}")
        print(f"    Passed: {result['passed']}")
        
        # Ensure result has required keys
        assert "orphan_count" in result, "Result missing 'orphan_count' key"
        assert "passed" in result, "Result missing 'passed' key"
        assert isinstance(result["orphan_count"], int), "orphan_count must be int"
        assert isinstance(result["passed"], bool), "passed must be bool"
    
    # Validate business expectations for each relationship
    # Note: Order depends on config.yaml ordering
    
    # First relationship: orders -> customers
    assert results[0]["orphan_count"] == 1, (
        "Expected 1 orphaned order in orders->customers relationship"
    )
    
    # Second relationship: order_line_items -> order_products (composite)
    assert results[1]["orphan_count"] == 1, (
        "Expected 1 orphaned line item in order_line_items->order_products relationship"
    )
    
    print("\nIntegration test PASSED: All FK checks completed successfully")



# ==============================================================================
# Test Runner (for direct execution)
# ==============================================================================

if __name__ == "__main__":
    # Run pytest with verbose output
    pytest.main([__file__, "-v", "-s"])
