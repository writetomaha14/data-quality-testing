"""
Test Suite for Schema Validation Checks

Tests for validating DataFrame schemas against expected schema definitions from config.yaml.
All test data is loaded from CSV files via fixtures - no inline test data.

Configuration:
    Expected schemas are defined in config.yaml under 'expected_schemas':
    
    expected_schemas:
      employees:
        - [employee_id, integer]
        - [name, string]
        - [manager_id, double]

Fixtures:
    All fixtures (spark_session, schema_config, table_registry) are provided
    by conftest.py and automatically available. No need to import them.

Usage:
    pytest test_schema_validation.py -v              # Run all schema tests
    pytest test_schema_validation.py -v -s           # Run with detailed output
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
    check_schema
)


# ==============================================================================
# Test Functions - Add tests here
# ==============================================================================

def test_exact_schema_match(table_registry: Dict[str, DataFrame], 
                           schema_config: Dict):
    """
    Test exact schema match: All columns present with correct types.
    
    This test validates that DataFrames loaded from test data match their
    expected schemas defined in config.yaml.
    
    Expected Behavior (based on config):
        - For each table in expected_schemas, the actual table should match exactly
        - The check should pass with no mismatches
    """
    print("Testing exact schema match for tables in config")
    
    expected_schemas = schema_config.get("expected_schemas", {})
    assert len(expected_schemas) > 0, "No expected_schemas found in config"
    
    # Test each table defined in config
    for table_name, expected_schema in expected_schemas.items():
        print(f"\n  Validating schema for table: {table_name}")
        
        # Check if table exists in registry
        if table_name not in table_registry:
            print(f"  Table '{table_name}' not found in table_registry, skipping")
            continue
        
        # Run the schema check
        result = check_schema(
            df=table_registry[table_name],
            expected_schema=expected_schema
        )
        
        # Validate results
        print(f"    Schema match passed: {result['passed']}")
        if not result['passed']:
            print(f"    Missing columns: {result['missing_columns']}")
            print(f"    Extra columns: {result['extra_columns']}")
            print(f"    Type mismatches: {result['type_mismatches']}")
        
        # Business expectation: Schema should match exactly
        assert result["passed"], (
            f"Table '{table_name}' schema mismatch. Expected schema from config, but found:\n"
            f"  Missing columns: {result['missing_columns']}\n"
            f"  Extra columns: {result['extra_columns']}\n"
            f"  Type mismatches: {result['type_mismatches']}"
        )


# ==============================================================================
# Test Runner (for direct execution)
# ==============================================================================

if __name__ == "__main__":
    # Run pytest with verbose output
    pytest.main([__file__, "-v", "-s"])