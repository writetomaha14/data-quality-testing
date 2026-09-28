"""
Tests for run_all_checks() — the Layer 3 Orchestrator

Verifies that the orchestrator:
1. Runs checks for all declared datasets and returns results for each
2. Only runs declared checks — omitted check types are skipped entirely
3. Returns empty results when datasets config is empty
4. Calculates DQ score when scoring config is present
5. Does NOT append a score when scoring config is absent
6. Every result dict includes a 'passed' boolean
"""

import sys
import logging
from pathlib import Path

sys.path.append('/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/src/checks')
from dq_checks import run_all_checks

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==============================================================================
# Helper: Build small inline DataFrames and configs for controlled testing
# ==============================================================================

def make_table_registry():
    """Create a small table registry with two clean datasets for testing."""
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
    ])
    customers_data = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com"),
    ]

    orders_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("total", DoubleType(), False),
    ])
    orders_data = [
        (101, 1, 100.00),
        (102, 2, 200.00),
        (103, 1, 150.00),
    ]

    return {
        "customers": spark.createDataFrame(customers_data, customers_schema),
        "orders": spark.createDataFrame(orders_data, orders_schema),
    }


def make_multi_dataset_config():
    """Config with two datasets, each declaring completeness + uniqueness."""
    return {
        "datasets": {
            "orders": {
                "completeness": {
                    "columns": ["order_id", "customer_id", "total"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["order_id"],
                },
            },
            "customers": {
                "completeness": {
                    "columns": ["customer_id", "name", "email"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["customer_id"],
                },
            },
        },
    }


def make_partial_config():
    """Config where one dataset only declares completeness."""
    return {
        "datasets": {
            "orders": {
                "completeness": {
                    "columns": ["order_id"],
                    "max_null_pct": 0,
                },
            },
        },
    }


def make_empty_config():
    """Config with no datasets declared."""
    return {"datasets": {}}


def make_scoring_config():
    """Config with DQ scoring enabled."""
    return {
        "datasets": {
            "orders": {
                "completeness": {
                    "columns": ["order_id"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["order_id"],
                },
            },
        },
        "dq_scoring": {
            "category_mapping": {
                "completeness": "completeness",
                "uniqueness": "uniqueness",
            },
            "weights": {
                "completeness": 50,
                "uniqueness": 50,
            },
            "grade_thresholds": {
                "A": 90,
                "B": 80,
                "C": 70,
                "D": 60,
                "F": 0,
            },
            "min_passing_score": 80,
        },
    }


# ==============================================================================
# Tests
# ==============================================================================

def test_multiple_datasets_all_return_results():
    """Config with two datasets returns results from both."""
    registry = make_table_registry()
    config = make_multi_dataset_config()

    results = run_all_checks(registry, config)

    # Both datasets should appear in results
    datasets_in_results = set(r.get("dataset") for r in results if "dataset" in r)
    assert "orders" in datasets_in_results, "orders results missing"
    assert "customers" in datasets_in_results, "customers results missing"

    # orders: 3 completeness + 1 uniqueness = 4
    # customers: 3 completeness + 1 uniqueness = 4
    # Total = 8
    assert len(results) == 8, f"Expected 8 results, got {len(results)}"

    print(f"\n✓ Test: Multiple datasets return results — {len(results)} checks across 2 datasets")


def test_only_declared_checks_run():
    """Dataset with only completeness declared — no other check types in results."""
    registry = make_table_registry()
    config = make_partial_config()

    results = run_all_checks(registry, config)

    check_types = set(r.get("check") for r in results)
    assert "completeness" in check_types, "completeness should be present"
    assert "uniqueness" not in check_types, "uniqueness should be skipped"
    assert "freshness" not in check_types, "freshness should be skipped"
    assert "consistency" not in check_types, "consistency should be skipped"
    assert "schema_validation" not in check_types, "schema should be skipped"

    # Only 1 completeness check (1 column declared)
    assert len(results) == 1, f"Expected 1 result, got {len(results)}"

    print(f"\n✓ Test: Only declared checks run — skipped all but completeness")


def test_empty_datasets_returns_empty():
    """Empty datasets config returns empty results list."""
    registry = make_table_registry()
    config = make_empty_config()

    results = run_all_checks(registry, config)
    assert results == [], f"Expected empty list, got {results}"

    print("\n✓ Test: Empty datasets config returns empty results")


def test_dq_score_calculated_when_config_present():
    """DQ score is calculated and appended when dq_scoring config is present."""
    registry = make_table_registry()
    config = make_scoring_config()

    results = run_all_checks(registry, config)

    # Last result should be the DQ score
    score_result = results[-1]
    assert score_result["check"] == "dq_score", f"Expected dq_score, got {score_result.get('check')}"
    assert "total_score" in score_result, "total_score missing"
    assert "grade" in score_result, "grade missing"
    assert "category_scores" in score_result, "category_scores missing"

    # All checks passed (no nulls, unique order_id) → score should be 100
    assert score_result["total_score"] == 100.0, f"Expected 100.0, got {score_result['total_score']}"
    assert score_result["grade"] == "A", f"Expected grade A, got {score_result['grade']}"
    assert score_result["passed"] is True, "Expected passed=True"

    print(f"\n✓ Test: DQ score calculated — score={score_result['total_score']}, grade={score_result['grade']}")


def test_no_scoring_config_no_score_appended():
    """When dq_scoring is absent from config, no score dict is appended."""
    registry = make_table_registry()
    config = make_multi_dataset_config()  # no dq_scoring key

    results = run_all_checks(registry, config)

    score_results = [r for r in results if r.get("check") == "dq_score"]
    assert len(score_results) == 0, f"Expected no dq_score, got {len(score_results)}"

    print("\n✓ Test: No scoring config → no score appended")


def test_all_results_have_passed_key():
    """Every check result dict should include a 'passed' boolean."""
    registry = make_table_registry()
    config = make_multi_dataset_config()

    results = run_all_checks(registry, config)

    for r in results:
        assert "passed" in r, f"Result missing 'passed' key: {r}"
        assert isinstance(r["passed"], bool), f"'passed' is not bool: {r['passed']}"

    print(f"\n✓ Test: All {len(results)} results have 'passed' boolean")


# ==============================================================================
# Run all tests when file is executed directly
# ==============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("RUNNING ORCHESTRATOR TESTS — run_all_checks()")
    print("=" * 70)

    tests = [
        test_multiple_datasets_all_return_results,
        test_only_declared_checks_run,
        test_empty_datasets_returns_empty,
        test_dq_score_calculated_when_config_present,
        test_no_scoring_config_no_score_appended,
        test_all_results_have_passed_key,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except (AssertionError, Exception) as e:
            print(f"\n❌ {test.__name__}: FAILED — {e}")
            failed += 1

    print(f"\n{'=' * 70}")
    print(f"ORCHESTRATOR TESTS COMPLETE — {passed} passed, {failed} failed")
    print(f"{'=' * 70}")