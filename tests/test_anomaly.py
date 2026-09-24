"""
Anomaly Detection Test Suite

This module tests anomaly detection functions — Z-score and IQR methods —
that flag values deviating significantly from the rest of the data.

Anomaly detection answers: "Are there values that deviate significantly from the distribution?"

Design Principle:
    Tests are data-agnostic and config-driven. No hard-coded counts, means, or
    thresholds. All expected values are derived from the DataFrames themselves,
    and all parameters (columns, thresholds, multipliers) come from config.yaml.
    To adapt the framework, edit only config.yaml — no test code changes needed.

Fixtures (provided by conftest.py):
    - spark_session: Spark session for the test run
    - anomaly_test_data: Sales DataFrame with injected anomalies (sales_anomaly_data.csv)
    - anomaly_config: Anomaly check rules loaded from config.yaml

Test Data:
    - sales_anomaly_data.csv: 16 rows of sales data
      13 normal rows (sales_amount 145-210, units_sold 20-45)
      3 anomalous rows:
        Row 14: sales_amount = 5000 (extreme high)
        Row 15: sales_amount = 2 (extreme low)
        Row 16: units_sold = 200 (extreme high)
    - config.yaml: anomaly_checks (z_score + iqr on sales_amount and units_sold)

Functions tested (from dq_checks.py):
    - check_z_score_anomaly(df, column_name, threshold)
    - check_iqr_anomaly(df, column_name, multiplier)
    - get_anomaly_rows(df, column_name, method, ...)
    - check_all_anomalies(df, anomaly_config)
"""

import sys
from pathlib import Path
from typing import List, Dict

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, mean as spark_mean, stddev as spark_stddev, abs as spark_abs


# ==============================================================================
# Path Configuration
# ==============================================================================

TEST_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = TEST_DIR.parent
CHECKS_DIR = PROJECT_ROOT / "src" / "checks"

# Add checks module to path
sys.path.insert(0, str(CHECKS_DIR))
from dq_checks import (
    check_z_score_anomaly,
    check_iqr_anomaly,
    get_anomaly_rows,
    check_all_anomalies,
)


# ==============================================================================
# Z-Score Anomaly Detection Tests
# ==============================================================================

def test_z_score_detects_high_outlier(anomaly_test_data: DataFrame,
                                       anomaly_config: List[Dict]):
    """Z-score anomaly detection flags the extreme high value (sales_amount=5000)"""
    z_config = next(c for c in anomaly_config if c["method"] == "z_score" and c["column"] == "sales_amount")
    threshold = z_config["threshold"]
    
    result = check_z_score_anomaly(anomaly_test_data, "sales_amount", threshold)
    
    assert result["check"] == "z_score_anomaly"
    assert result["column"] == "sales_amount"
    assert result["threshold"] == threshold
    assert result["anomaly_count"] > 0, "Should detect at least one anomaly (sales_amount=5000)"
    assert result["passed"] == False


def test_z_score_reports_correct_stats(anomaly_test_data: DataFrame,
                                         anomaly_config: List[Dict]):
    """Z-score function correctly reports mean and stddev from the data"""
    z_config = next(c for c in anomaly_config if c["method"] == "z_score" and c["column"] == "sales_amount")
    threshold = z_config["threshold"]
    
    # Independently compute mean and stddev
    stats = anomaly_test_data.agg(
        spark_mean(col("sales_amount")).alias("mean"),
        spark_stddev(col("sales_amount")).alias("stddev"),
    ).collect()[0]
    expected_mean = round(stats["mean"], 2)
    expected_stddev = round(stats["stddev"], 2)
    
    result = check_z_score_anomaly(anomaly_test_data, "sales_amount", threshold)
    
    assert result["mean"] == expected_mean
    assert result["stddev"] == expected_stddev
    assert result["total_rows"] == anomaly_test_data.count()


def test_z_score_detects_units_outlier(anomaly_test_data: DataFrame,
                                         anomaly_config: List[Dict]):
    """Z-score anomaly detection flags the extreme units_sold=200"""
    z_config = next(c for c in anomaly_config if c["method"] == "z_score" and c["column"] == "units_sold")
    threshold = z_config["threshold"]
    
    result = check_z_score_anomaly(anomaly_test_data, "units_sold", threshold)
    
    assert result["check"] == "z_score_anomaly"
    assert result["column"] == "units_sold"
    assert result["anomaly_count"] > 0, "Should detect units_sold=200 as anomalous"
    assert result["passed"] == False


# ==============================================================================
# IQR Anomaly Detection Tests
# ==============================================================================

def test_iqr_detects_both_high_and_low_outliers(anomaly_test_data: DataFrame,
                                                    anomaly_config: List[Dict]):
    """IQR anomaly detection flags both sales_amount=5000 and sales_amount=2"""
    iqr_config = next(c for c in anomaly_config if c["method"] == "iqr" and c["column"] == "sales_amount")
    multiplier = iqr_config["multiplier"]
    
    result = check_iqr_anomaly(anomaly_test_data, "sales_amount", multiplier)
    
    assert result["check"] == "iqr_anomaly"
    assert result["column"] == "sales_amount"
    assert result["anomaly_count"] >= 2, "IQR should catch both 5000 and 2"
    assert result["passed"] == False


def test_iqr_reports_correct_bounds(anomaly_test_data: DataFrame,
                                      anomaly_config: List[Dict]):
    """IQR function correctly computes Q1, Q3, IQR, and bounds"""
    iqr_config = next(c for c in anomaly_config if c["method"] == "iqr" and c["column"] == "sales_amount")
    multiplier = iqr_config["multiplier"]
    
    # Independently compute quantiles
    quantiles = anomaly_test_data.stat.approxQuantile("sales_amount", [0.25, 0.75], 0.01)
    expected_q1 = round(quantiles[0], 2)
    expected_q3 = round(quantiles[1], 2)
    expected_iqr = round(expected_q3 - expected_q1, 2)
    expected_lower = round(expected_q1 - multiplier * expected_iqr, 2)
    expected_upper = round(expected_q3 + multiplier * expected_iqr, 2)
    
    result = check_iqr_anomaly(anomaly_test_data, "sales_amount", multiplier)
    
    assert result["q1"] == expected_q1
    assert result["q3"] == expected_q3
    assert result["iqr"] == expected_iqr
    assert result["lower_bound"] == expected_lower
    assert result["upper_bound"] == expected_upper


def test_iqr_detects_units_outlier(anomaly_test_data: DataFrame,
                                     anomaly_config: List[Dict]):
    """IQR anomaly detection flags units_sold=200"""
    iqr_config = next(c for c in anomaly_config if c["method"] == "iqr" and c["column"] == "units_sold")
    multiplier = iqr_config["multiplier"]
    
    result = check_iqr_anomaly(anomaly_test_data, "units_sold", multiplier)
    
    assert result["check"] == "iqr_anomaly"
    assert result["anomaly_count"] > 0, "Should detect units_sold=200 as anomalous"
    assert result["passed"] == False


# ==============================================================================
# get_anomaly_rows Tests
# ==============================================================================

def test_get_anomaly_rows_z_score(anomaly_test_data: DataFrame,
                                    anomaly_config: List[Dict]):
    """get_anomaly_rows with z_score method returns actual anomalous rows"""
    z_config = next(c for c in anomaly_config if c["method"] == "z_score" and c["column"] == "units_sold")
    threshold = z_config["threshold"]
    
    anomaly_rows = get_anomaly_rows(anomaly_test_data, "units_sold", method="z_score", threshold=threshold)
    actual_count = anomaly_rows.count()
    
    # Compare against check function's count
    check_result = check_z_score_anomaly(anomaly_test_data, "units_sold", threshold)
    assert actual_count == check_result["anomaly_count"]
    assert actual_count > 0, "Should return at least one anomalous row"


def test_get_anomaly_rows_iqr(anomaly_test_data: DataFrame,
                                anomaly_config: List[Dict]):
    """get_anomaly_rows with iqr method returns actual anomalous rows"""
    iqr_config = next(c for c in anomaly_config if c["method"] == "iqr" and c["column"] == "sales_amount")
    multiplier = iqr_config["multiplier"]
    
    anomaly_rows = get_anomaly_rows(anomaly_test_data, "sales_amount", method="iqr", multiplier=multiplier)
    actual_count = anomaly_rows.count()
    
    # Compare against check function's count
    check_result = check_iqr_anomaly(anomaly_test_data, "sales_amount", multiplier)
    assert actual_count == check_result["anomaly_count"]
    assert actual_count > 0, "Should return at least one anomalous row"


def test_get_anomaly_rows_invalid_method(anomaly_test_data: DataFrame):
    """get_anomaly_rows raises ValueError for unknown method"""
    with pytest.raises(ValueError, match="Unknown anomaly method"):
        get_anomaly_rows(anomaly_test_data, "sales_amount", method="invalid")


# ==============================================================================
# Batch Anomaly Detection Tests
# ==============================================================================

def test_check_all_anomalies_from_config(anomaly_test_data: DataFrame,
                                            anomaly_config: List[Dict]):
    """Batch anomaly detection runs all checks from YAML config"""
    results = check_all_anomalies(anomaly_test_data, anomaly_config)
    
    # Should return one result per config entry
    assert len(results) == len(anomaly_config)
    # Each result should have a check type and column
    for r in results:
        assert "check" in r
        assert "column" in r
        assert "passed" in r


def test_check_all_anomalies_detects_failures(anomaly_test_data: DataFrame,
                                                  anomaly_config: List[Dict]):
    """Batch anomaly detection detects at least one failure on anomalous data"""
    results = check_all_anomalies(anomaly_test_data, anomaly_config)
    
    # At least one check should fail (detect anomalies)
    failed_checks = [r for r in results if not r["passed"]]
    assert len(failed_checks) > 0, "At least one anomaly check should fail on anomalous data"


# ==============================================================================
# Test Runner
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, '-v', '-s'])