"""
Referential Integrity Test Suite

This module tests the referential integrity checking functions from dq_checks module.
Verifies that orphaned records (child records without matching parent records) are 
correctly detected and reported.

Test Coverage:
- Detection of orphaned records in child tables
- Validation of clean relationships (no orphans)
- Consistency between check and get functions
- Null foreign key handling behavior

Test Data:
- customers.csv: Parent table with customer records
- orders.csv: Child table with order records (includes orphaned orders)
"""

# Import required libraries
import sys
import logging
from pyspark.sql import SparkSession

# Add DQ checks module to path
sys.path.append('/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/src/checks')
from dq_checks import check_referential_integrity,  get_orphaned_rows


# ==============================================================================
# Test Configuration
# ==============================================================================

base_path = "/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/tests/test_data"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================================================================
# Test Data Loading Functions
# ==============================================================================

# Load customers test data from CSV
def load_customers():
    spark = SparkSession.builder.getOrCreate()
    return spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/customers.csv")

# Load orders test data from CSV
def load_orders():
    spark = SparkSession.builder.getOrCreate()
    return spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/orders.csv")

# ==============================================================================
# Test Cases
# ==============================================================================

def test_referential_integrity_finds_orphan():
    """Verify function detects orphaned records when they exist"""
    orders = load_orders()
    customers = load_customers()
    
    print(f"\n🔍 Running test with {orders.count()} orders and {customers.count()} customers")
    result = check_referential_integrity(orders, "customer_id", customers, "customer_id")
    print(f"📊 Referential integrity check result: {result}")
    
    # Test behavior: orphans should be detected
    assert result["orphan_count"] > 0, "Test data should contain orphaned records"
    assert result["passed"] == False, "Check should fail when orphans exist"

def test_referential_integrity_clean_relationship():
    """Verify function passes when no orphans exist"""
    spark = SparkSession.builder.getOrCreate()
    customers = load_customers()
    clean_orders = spark.createDataFrame([(1, 101), (2, 102)], ["order_id", "customer_id"])
    
    print(f"\n🔍 Running clean relationship test with {clean_orders.count()} orders")
    result = check_referential_integrity(clean_orders, "customer_id", customers, "customer_id")
    print(f"📊 Clean relationship test result: {result}")
    
    assert result["orphan_count"] == 0, "Clean data should have no orphans"
    assert result["passed"] == True, "Check should pass when no orphans exist"

def test_get_orphaned_rows_returns_correct_rows():
    """Verify get_orphaned_rows returns matching count from check function"""
    orders = load_orders()
    customers = load_customers()
    
    print(f"\n🔍 Testing get_orphaned_rows function")
    # Both functions should agree on orphan count
    result = check_referential_integrity(orders, "customer_id", customers, "customer_id")
    orphans = get_orphaned_rows(orders, "customer_id", customers, "customer_id")
    print(f"📊 Check function found: {result['orphan_count']} orphans")
    print(f"📊 Get function returned: {orphans.count()} orphan rows")
    
    assert orphans.count() == result["orphan_count"], "Orphan count should match between check and get functions"
    assert orphans.count() > 0, "Test data should contain orphaned records"

def test_null_foreign_key_documented_behavior():
    """Verify null foreign keys are treated as orphans"""
    spark = SparkSession.builder.getOrCreate()
    customers = load_customers()
    orders_with_null = spark.createDataFrame([(1, 101), (2, None)], ["order_id", "customer_id"])
    
    print(f"\n🔍 Testing null foreign key behavior")
    result = check_referential_integrity(orders_with_null, "customer_id", customers, "customer_id")
    print(f"📊 Result with null FK: {result}")
    
    # Documenting the real, current behavior: a null FK IS counted as an orphan
    assert result["orphan_count"] == 1, "Null foreign key should be counted as orphan"
    assert result["passed"] == False, "Check should fail when null FK exists"

# ==============================================================================
# Test Runner
# ==============================================================================

if __name__ == "__main__":
    import pytest
    # Run pytest with verbose output and show logging
    # -v = verbose (show test names)
    # -s = show stdout/stderr (print statements and logging)
    # --log-cli-level=INFO = show INFO level logs in console
    pytest.main([__file__, '-v', '-s', '--log-cli-level=INFO'])
