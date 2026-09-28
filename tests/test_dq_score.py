"""
Data Quality Score Test Suite

This module tests the calculate_dq_score() function that aggregates individual
check results into a composite score (0-100) with a letter grade.

The DQ score answers: "What is the overall health of our data across all checks?"

Design Principle:
    Tests are config-driven. Scoring weights, category mapping, grade thresholds,
    and the minimum passing score all come from config.yaml via the scoring_config
    fixture. No hard-coded scores or grades in the test code.

Fixtures (provided by conftest.py):
    - scoring_config: DQ scoring configuration loaded from config.yaml

Functions tested (from dq_checks.py):
    - calculate_dq_score(check_results, scoring_config)
"""

import sys
from pathlib import Path
from typing import Dict

import pytest


# ==============================================================================
# Path Configuration
# ==============================================================================

TEST_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = TEST_DIR.parent
CHECKS_DIR = PROJECT_ROOT / "src" / "checks"

# Add checks module to path
sys.path.insert(0, str(CHECKS_DIR))
from dq_checks import calculate_dq_score


# ==============================================================================
# Helper: Build Mock Check Results
# ==============================================================================

def _result(check_name, passed):
    """Create a minimal check result dict for testing."""
    return {"check": check_name, "passed": passed}


# ==============================================================================
# Basic Scoring Tests
# ==============================================================================

def test_all_passed_gives_perfect_score(scoring_config: Dict):
    """When every check passes, the total score should be 100 and grade A."""
    results = [
        _result("completeness", True),
        _result("uniqueness", True),
        _result("referential_integrity", True),
        _result("schema_validation", True),
        _result("freshness", True),
        _result("consistency", True),
        _result("record_count_reconciliation", True),
        _result("z_score_anomaly", True),
    ]

    score = calculate_dq_score(results, scoring_config)

    assert score["check"] == "dq_score"
    assert score["total_score"] == 100.0
    assert score["grade"] == "A"
    assert score["passed"] is True


def test_all_failed_gives_zero_score(scoring_config: Dict):
    """When every check fails, the total score should be 0 and grade F."""
    results = [
        _result("completeness", False),
        _result("uniqueness", False),
        _result("referential_integrity", False),
        _result("schema_validation", False),
        _result("freshness", False),
        _result("consistency", False),
        _result("record_count_reconciliation", False),
        _result("z_score_anomaly", False),
    ]

    score = calculate_dq_score(results, scoring_config)

    assert score["total_score"] == 0.0
    assert score["grade"] == "F"
    assert score["passed"] is False


def test_mixed_results_partial_score(scoring_config: Dict):
    """Half the categories pass, half fail — score should be roughly 50."""
    results = [
        _result("completeness", True),
        _result("uniqueness", True),
        _result("referential_integrity", True),
        _result("schema_validation", True),
        _result("freshness", False),
        _result("consistency", False),
        _result("record_count_reconciliation", False),
        _result("z_score_anomaly", False),
    ]

    score = calculate_dq_score(results, scoring_config)

    assert 0 < score["total_score"] < 100
    assert score["grade"] in ("B", "C", "D", "F")


# ==============================================================================
# Category Mapping Tests
# ==============================================================================

def test_category_mapping_groups_anomaly_checks(scoring_config: Dict):
    """z_score_anomaly and iqr_anomaly should be grouped into anomaly_detection."""
    results = [
        _result("z_score_anomaly", True),
        _result("iqr_anomaly", False),
    ]

    score = calculate_dq_score(results, scoring_config)

    # Both should map to anomaly_detection category
    assert "anomaly_detection" in score["category_scores"]
    cat = score["category_scores"]["anomaly_detection"]
    assert cat["total_checks"] == 2
    assert cat["passed_checks"] == 1
    assert cat["pass_rate"] == 50.0


def test_category_mapping_groups_reconciliation_checks(scoring_config: Dict):
    """All reconciliation check types should group into the reconciliation category."""
    results = [
        _result("record_count_reconciliation", True),
        _result("sum_reconciliation", True),
        _result("row_level_reconciliation", False),
    ]

    score = calculate_dq_score(results, scoring_config)

    assert "reconciliation" in score["category_scores"]
    cat = score["category_scores"]["reconciliation"]
    assert cat["total_checks"] == 3
    assert cat["passed_checks"] == 2
    assert cat["pass_rate"] == round(2 / 3 * 100, 1)


def test_category_mapping_groups_uniqueness_composite(scoring_config: Dict):
    """uniqueness and uniqueness_composite should group into the uniqueness category."""
    results = [
        _result("uniqueness", True),
        _result("uniqueness_composite", False),
    ]

    score = calculate_dq_score(results, scoring_config)

    assert "uniqueness" in score["category_scores"]
    cat = score["category_scores"]["uniqueness"]
    assert cat["total_checks"] == 2
    assert cat["passed_checks"] == 1
    assert cat["pass_rate"] == 50.0


def test_unknown_check_name_uses_name_as_category(scoring_config: Dict):
    """A check name not in category_mapping should use itself as the category."""
    results = [_result("some_new_check", True)]

    score = calculate_dq_score(results, scoring_config)

    assert "some_new_check" in score["category_scores"]


# ==============================================================================
# Grade Threshold Tests
# ==============================================================================

def test_grade_a_at_threshold(scoring_config: Dict):
    """Score >= 90 should produce grade A."""
    # All categories with 100% pass rate except one small one
    results = [
        _result("completeness", True),
        _result("uniqueness", True),
        _result("referential_integrity", True),
        _result("schema_validation", True),
        _result("freshness", True),
        _result("consistency", True),
        _result("record_count_reconciliation", True),
        _result("z_score_anomaly", True),
        _result("iqr_anomaly", True),
    ]

    score = calculate_dq_score(results, scoring_config)
    assert score["grade"] == "A"


def test_grade_f_when_all_fail(scoring_config: Dict):
    """Score of 0 should produce grade F."""
    results = [_result("completeness", False)]

    score = calculate_dq_score(results, scoring_config)
    assert score["grade"] == "F"


def test_grade_thresholds_from_config(scoring_config: Dict):
    """Grade thresholds should match config values."""
    thresholds = scoring_config["grade_thresholds"]
    assert "A" in thresholds
    assert "F" in thresholds
    assert thresholds["A"] >= thresholds["B"] >= thresholds["C"] >= thresholds["D"] >= thresholds["F"]


# ==============================================================================
# Weight Application Tests
# ==============================================================================

def test_weights_sum_to_100(scoring_config: Dict):
    """Configured category weights should sum to 100."""
    weights = scoring_config["weights"]
    assert sum(weights.values()) == 100


def test_weighted_score_calculation(scoring_config: Dict):
    """A category with weight 20 and 50% pass rate contributes 10 to the total."""
    results = [
        _result("completeness", True),
        _result("completeness", False),
    ]

    score = calculate_dq_score(results, scoring_config)

    weight = scoring_config["weights"]["completeness"]
    expected_contribution = 50.0 * (weight / 100)

    cat = score["category_scores"]["completeness"]
    assert cat["weight"] == weight
    assert cat["pass_rate"] == 50.0
    assert cat["weighted_score"] == round(expected_contribution, 1)


# ==============================================================================
# Edge Case Tests
# ==============================================================================

def test_empty_results_gives_zero_score(scoring_config: Dict):
    """An empty list of check results should give score 0 and grade F."""
    score = calculate_dq_score([], scoring_config)

    assert score["total_score"] == 0.0
    assert score["grade"] == "F"
    assert score["passed"] is False
    assert score["category_scores"] == {}


def test_single_passing_check(scoring_config: Dict):
    """A single passing check should give a score equal to its weight."""
    results = [_result("completeness", True)]

    score = calculate_dq_score(results, scoring_config)

    weight = scoring_config["weights"]["completeness"]
    assert score["total_score"] == float(weight)


def test_single_failing_check(scoring_config: Dict):
    """A single failing check should give a score of 0 for that category."""
    results = [_result("completeness", False)]

    score = calculate_dq_score(results, scoring_config)

    assert score["total_score"] == 0.0


def test_min_passing_score_from_config(scoring_config: Dict):
    """The passed flag should use min_passing_score from config."""
    min_pass = scoring_config["min_passing_score"]

    # All pass = score 100, should pass
    all_pass = [_result(c, True) for c in ["completeness", "uniqueness",
        "referential_integrity", "schema_validation", "freshness",
        "consistency", "record_count_reconciliation", "z_score_anomaly"]]
    score = calculate_dq_score(all_pass, scoring_config)
    assert score["passed"] is True
    assert score["total_score"] >= min_pass


def test_result_dict_has_all_required_fields(scoring_config: Dict):
    """The returned dict should have all expected keys."""
    results = [_result("completeness", True)]
    score = calculate_dq_score(results, scoring_config)

    assert "check" in score
    assert "total_score" in score
    assert "grade" in score
    assert "total_weight_applied" in score
    assert "category_scores" in score
    assert "passed" in score


def test_category_scores_has_breakdown_fields(scoring_config: Dict):
    """Each category in category_scores should have all detail fields."""
    results = [_result("completeness", True), _result("completeness", False)]
    score = calculate_dq_score(results, scoring_config)

    cat = score["category_scores"]["completeness"]
    assert "total_checks" in cat
    assert "passed_checks" in cat
    assert "pass_rate" in cat
    assert "weight" in cat
    assert "weighted_score" in cat