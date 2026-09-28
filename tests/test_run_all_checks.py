"""
Tests for run_all_checks() — the Layer 3 Orchestrator

Tests across three completely different data domains (healthcare, ecommerce,
finance) to prove the framework is dataset-agnostic. Test data is loaded
from CSV files in tests/test_data/ — no inline DataFrames.

Verifies that the orchestrator:
1. Runs checks for all declared datasets across different domains
2. Each domain only gets the checks declared for it (different per domain)
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TEST_DATA_DIR = "/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/tests/test_data"


# ==============================================================================
# Helper: Load CSV test data and build domain-specific configs
# ==============================================================================

def load_csv(filename):
    """Load a CSV file from tests/test_data/ as a Spark DataFrame."""
    return spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(f"{TEST_DATA_DIR}/{filename}")


def make_table_registry():
    """Load test data for three different domains: healthcare, ecommerce, finance."""
    return {
        "healthcare_patients": load_csv("healthcare_patients.csv"),
        "ecommerce_orders": load_csv("ecommerce_orders.csv"),
        "finance_transactions": load_csv("finance_transactions.csv"),
    }


def make_multi_domain_config():
    """Config with three different domains, each declaring different checks."""
    return {
        "datasets": {
            "healthcare_patients": {
                "completeness": {
                    "columns": ["patient_id", "name", "diagnosis"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["patient_id"],
                },
                "consistency": {
                    "rules": [
                        {"name": "discharge_after_admission", "expression": "discharge_date >= admission_date"},
                    ],
                },
            },
            "ecommerce_orders": {
                "completeness": {
                    "columns": ["order_id", "customer_id", "total"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["order_id"],
                },
                "consistency": {
                    "rules": [
                        {"name": "ship_after_order", "expression": "ship_date >= order_date"},
                    ],
                },
                "business_rules": {
                    "rules": [
                        {"name": "valid_status", "expression": "status IN ('pending', 'shipped', 'delivered', 'cancelled')"},
                        {"name": "discount_within_total", "expression": "discount <= total"},
                    ],
                },
            },
            "finance_transactions": {
                "completeness": {
                    "columns": ["transaction_id", "account_id", "amount"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["transaction_id"],
                },
                "business_rules": {
                    "rules": [
                        {"name": "valid_transaction_type", "expression": "transaction_type IN ('credit', 'debit', 'transfer')"},
                    ],
                },
            },
        },
    }


def make_partial_config():
    """Config where one domain only declares completeness."""
    return {
        "datasets": {
            "healthcare_patients": {
                "completeness": {
                    "columns": ["patient_id"],
                    "max_null_pct": 0,
                },
            },
        },
    }


def make_empty_config():
    """Config with no datasets declared."""
    return {"datasets": {}}


def make_scoring_config():
    """Config with DQ scoring enabled across three domains."""
    return {
        "datasets": {
            "healthcare_patients": {
                "completeness": {
                    "columns": ["patient_id"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["patient_id"],
                },
            },
            "finance_transactions": {
                "completeness": {
                    "columns": ["transaction_id"],
                    "max_null_pct": 0,
                },
                "uniqueness": {
                    "columns": ["transaction_id"],
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

def test_three_domains_all_return_results():
    """Config with healthcare, ecommerce, and finance returns results from all three."""
    registry = make_table_registry()
    config = make_multi_domain_config()

    results = run_all_checks(registry, config)

    # All three domains should appear in results
    datasets_in_results = set(r.get("dataset") for r in results if "dataset" in r)
    assert "healthcare_patients" in datasets_in_results, "healthcare results missing"
    assert "ecommerce_orders" in datasets_in_results, "ecommerce results missing"
    assert "finance_transactions" in datasets_in_results, "finance results missing"

    # healthcare: 3 completeness + 1 uniqueness + 1 consistency = 5
    # ecommerce: 3 completeness + 1 uniqueness + 1 consistency + 2 business_rules = 7
    # finance: 3 completeness + 1 uniqueness + 1 business_rule = 5
    # Total = 17
    assert len(results) == 17, f"Expected 17 results, got {len(results)}"

    print(f"\n✓ Test: Three domains return results — {len(results)} checks across healthcare, ecommerce, finance")


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

    print(f"\n✓ Test: Only declared checks run — healthcare domain, completeness only")


def test_different_domains_get_different_checks():
    """Each domain gets different checks based on its config declaration."""
    registry = make_table_registry()
    config = make_multi_domain_config()

    results = run_all_checks(registry, config)

    # Group results by dataset
    by_dataset = {}
    for r in results:
        ds = r.get("dataset")
        if ds:
            by_dataset.setdefault(ds, []).append(r)

    # Healthcare has a consistency rule (discharge_after_admission)
    healthcare_rules = [r.get("rule_name") for r in by_dataset["healthcare_patients"] if r.get("check") == "consistency"]
    assert "discharge_after_admission" in healthcare_rules, "healthcare should have discharge_after_admission rule"

    # Ecommerce has business rules (valid_status, discount_within_total)
    ecommerce_rules = [r.get("rule_name") for r in by_dataset["ecommerce_orders"] if r.get("check") == "consistency"]
    assert "ship_after_order" in ecommerce_rules, "ecommerce should have ship_after_order rule"
    assert "valid_status" in ecommerce_rules, "ecommerce should have valid_status business rule"
    assert "discount_within_total" in ecommerce_rules, "ecommerce should have discount_within_total business rule"

    # Finance has a business rule (valid_transaction_type) but NO consistency rule
    finance_rules = [r.get("rule_name") for r in by_dataset["finance_transactions"] if r.get("check") == "consistency"]
    assert "valid_transaction_type" in finance_rules, "finance should have valid_transaction_type business rule"
    assert "discharge_after_admission" not in finance_rules, "finance should NOT have healthcare's consistency rule"
    assert "ship_after_order" not in finance_rules, "finance should NOT have ecommerce's consistency rule"

    # Finance does NOT have freshness or schema checks (not declared)
    finance_check_types = set(r.get("check") for r in by_dataset["finance_transactions"])
    assert "freshness" not in finance_check_types, "finance should NOT have freshness"
    assert "schema_validation" not in finance_check_types, "finance should NOT have schema"

    print(f"\n✓ Test: Different domains get different checks — verified per-domain rule dispatch")


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

    # All checks passed (no nulls, unique ids) → score should be 100
    assert score_result["total_score"] == 100.0, f"Expected 100.0, got {score_result['total_score']}"
    assert score_result["grade"] == "A", f"Expected grade A, got {score_result['grade']}"
    assert score_result["passed"] is True, "Expected passed=True"

    print(f"\n✓ Test: DQ score calculated — score={score_result['total_score']}, grade={score_result['grade']}")


def test_empty_datasets_returns_empty():
    """Empty datasets config returns empty results list."""
    registry = make_table_registry()
    config = make_empty_config()

    results = run_all_checks(registry, config)
    assert results == [], f"Expected empty list, got {results}"

    print("\n✓ Test: Empty datasets config returns empty results")


def test_no_scoring_config_no_score_appended():
    """When dq_scoring is absent from config, no score dict is appended."""
    registry = make_table_registry()
    config = make_multi_domain_config()  # no dq_scoring key

    results = run_all_checks(registry, config)

    score_results = [r for r in results if r.get("check") == "dq_score"]
    assert len(score_results) == 0, f"Expected no dq_score, got {len(score_results)}"

    print("\n✓ Test: No scoring config → no score appended")


def test_all_results_have_passed_key():
    """Every check result dict should include a 'passed' boolean."""
    registry = make_table_registry()
    config = make_multi_domain_config()

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
        test_three_domains_all_return_results,
        test_only_declared_checks_run,
        test_different_domains_get_different_checks,
        test_dq_score_calculated_when_config_present,
        test_empty_datasets_returns_empty,
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