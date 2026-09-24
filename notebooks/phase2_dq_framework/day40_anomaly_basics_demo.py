# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Anomaly Basics Demo - Overview
# MAGIC %md
# MAGIC # Day 40: Anomaly Detection Basics Demo
# MAGIC
# MAGIC This notebook demonstrates how to detect anomalous (outlier) values in numeric columns using two foundational statistical methods.
# MAGIC
# MAGIC ## Overview
# MAGIC Anomaly detection answers: "Are there values in this data that deviate significantly from the rest?" Unlike rule-based checks (completeness, uniqueness, consistency), anomaly detection is **statistical** — it looks for values that are unusual relative to the distribution of the data itself, without needing pre-defined thresholds for every column.
# MAGIC
# MAGIC ## What This Demo Covers
# MAGIC * Loading sales test data from a CSV file (with injected anomalies)
# MAGIC * Loading anomaly detection rules from YAML configuration (config.yaml)
# MAGIC * Detecting outliers using the **Z-score method** with `check_z_score_anomaly()`
# MAGIC * Detecting outliers using the **IQR (interquartile range) method** with `check_iqr_anomaly()`
# MAGIC * Retrieving actual anomalous rows using `get_anomaly_rows()`
# MAGIC * Running all anomaly checks in batch using `check_all_anomalies()`
# MAGIC * Saving results to a Delta table for tracking and reporting
# MAGIC
# MAGIC ## Test Scenario
# MAGIC * **Dataset**: sales_anomaly_data.csv (16 rows)
# MAGIC   * 13 normal rows: sales_amount 145–210, units_sold 20–45
# MAGIC   * 3 anomalous rows:
# MAGIC     * Row 14: sales_amount = 5000 (extreme high outlier)
# MAGIC     * Row 15: sales_amount = 2 (extreme low outlier)
# MAGIC     * Row 16: units_sold = 200 (extreme high outlier)
# MAGIC * **Config**: Loaded from config.yaml → anomaly_checks
# MAGIC   * `z_score` on sales_amount (threshold 2.0)
# MAGIC   * `z_score` on units_sold (threshold 2.0)
# MAGIC   * `iqr` on sales_amount (multiplier 1.5)
# MAGIC   * `iqr` on units_sold (multiplier 1.5)
# MAGIC
# MAGIC ## Key Functions
# MAGIC * `check_z_score_anomaly(df, column_name, threshold=3.0)` - Flag values > N standard deviations from the mean
# MAGIC * `check_iqr_anomaly(df, column_name, multiplier=1.5)` - Flag values outside Q1 - 1.5×IQR or Q3 + 1.5×IQR
# MAGIC * `get_anomaly_rows(df, column_name, method, ...)` - Return the actual anomalous rows
# MAGIC * `check_all_anomalies(df, anomaly_config)` - Run all anomaly checks from config
# MAGIC
# MAGIC ## Teaching Point: Z-score vs IQR
# MAGIC The Z-score method is sensitive to extreme outliers — a single very large value inflates the mean and stddev, which can mask other outliers. The IQR method uses percentiles, which are **robust** to extreme values. This demo shows both methods and compares their results.

# COMMAND ----------

# DBTITLE 1,Demo: Anomaly Detection
"""
Anomaly Detection Basics Demo

This script demonstrates how to detect anomalous values in numeric columns
using Z-score and IQR methods. It uses anomaly detection functions from
dq_checks.py and loads all configuration from config.yaml.

Anomaly Checks (from config.yaml):
    1. z_score on sales_amount (threshold 2.0)
    2. z_score on units_sold (threshold 2.0)
    3. iqr on sales_amount (multiplier 1.5)
    4. iqr on units_sold (multiplier 1.5)

Expected Results:
    - Z-score catches extreme outliers (sales_amount=5000, units_sold=200)
      but may miss sales_amount=2 because the mean is inflated by 5000
    - IQR catches both sales_amount=2 and sales_amount=5000 because
      percentiles are robust to extreme values
"""

# ============================================================================
# 1. SETUP: Import required libraries
# ============================================================================
import sys
import yaml
import os

# ============================================================================
# 2. DYNAMIC PATH CONFIGURATION
# ============================================================================
# Get the base repository path dynamically to avoid hard-coded paths
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# Go up three levels: day40 -> phase2_dq_framework -> notebooks -> data-quality-testing (base)
base_path = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))

# Prepend /Workspace for filesystem access (notebookPath returns workspace path)
ws_base = os.path.join("/Workspace", base_path.lstrip("/"))

# Define paths
CHECKS_DIR = os.path.join(ws_base, "src", "checks")
CONFIG_DIR = os.path.join(ws_base, "src", "config")
TEST_DATA_DIR = os.path.join(ws_base, "tests", "test_data")

# Prevent __pycache__ creation on FUSE mount
sys.dont_write_bytecode = True

# Add checks module to path
sys.path.append(CHECKS_DIR)

print(f"📁 Base path: {ws_base}")
print(f"📁 Checks directory: {CHECKS_DIR}")
print(f"📁 Config directory: {CONFIG_DIR}")
print(f"📁 Test data directory: {TEST_DATA_DIR}")

# Import anomaly detection functions
from dq_checks import (
    check_z_score_anomaly,
    check_iqr_anomaly,
    get_anomaly_rows,
    check_all_anomalies,
)

print("✅ All anomaly detection functions imported successfully")

# ============================================================================
# 3. LOAD CONFIGURATION FROM YAML
# ============================================================================
config_path = os.path.join(CONFIG_DIR, "config.yaml")
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

anomaly_config = config.get('anomaly_checks', [])

print(f"\n📋 Loaded {len(anomaly_config)} anomaly checks from config.yaml:")
for check in anomaly_config:
    method = check['method']
    column = check['column']
    param = check.get('threshold', check.get('multiplier', ''))
    param_name = 'threshold' if 'threshold' in check else 'multiplier'
    print(f"   - {method} on '{column}' ({param_name}={param})")

# ============================================================================
# 4. LOAD TEST DATA
# ============================================================================
print("\n" + "=" * 70)
print("Loading test data...")
print("=" * 70)

sales_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(os.path.join(TEST_DATA_DIR, "sales_anomaly_data.csv"))

print(f"\n📊 Sales Data ({sales_df.count()} rows):")
sales_df.display()

# ============================================================================
# 5. Z-SCORE ANOMALY DETECTION
# ============================================================================
print("\n" + "=" * 70)
print("5. Z-SCORE ANOMALY DETECTION")
print("=" * 70)
print("Z-score = (value - mean) / stddev")
print("Values with |Z-score| > threshold are flagged as anomalies.\n")

# Check sales_amount with Z-score (threshold from config: 2.0)
z_sales_threshold = next(c for c in anomaly_config if c["method"] == "z_score" and c["column"] == "sales_amount")["threshold"]
z_sales_result = check_z_score_anomaly(sales_df, "sales_amount", z_sales_threshold)
print(f"📊 Z-score on 'sales_amount' (threshold={z_sales_threshold}):")
print(f"   Mean: {z_sales_result['mean']}, Stddev: {z_sales_result['stddev']}")
print(f"   Anomalies: {z_sales_result['anomaly_count']} ({z_sales_result['anomaly_pct']}%)")
print(f"   Passed: {z_sales_result['passed']}")

# Check units_sold with Z-score (threshold from config: 2.0)
z_units_threshold = next(c for c in anomaly_config if c["method"] == "z_score" and c["column"] == "units_sold")["threshold"]
z_units_result = check_z_score_anomaly(sales_df, "units_sold", z_units_threshold)
print(f"\n📊 Z-score on 'units_sold' (threshold={z_units_threshold}):")
print(f"   Mean: {z_units_result['mean']}, Stddev: {z_units_result['stddev']}")
print(f"   Anomalies: {z_units_result['anomaly_count']} ({z_units_result['anomaly_pct']}%)")
print(f"   Passed: {z_units_result['passed']}")

# ============================================================================
# 6. IQR ANOMALY DETECTION
# ============================================================================
print("\n" + "=" * 70)
print("6. IQR ANOMALY DETECTION")
print("=" * 70)
print("IQR = Q3 - Q1 (interquartile range)")
print("Values outside [Q1 - multiplier×IQR, Q3 + multiplier×IQR] are anomalies.\n")

# Check sales_amount with IQR (multiplier from config: 1.5)
iqr_sales_multiplier = next(c for c in anomaly_config if c["method"] == "iqr" and c["column"] == "sales_amount")["multiplier"]
iqr_sales_result = check_iqr_anomaly(sales_df, "sales_amount", iqr_sales_multiplier)
print(f"📊 IQR on 'sales_amount' (multiplier={iqr_sales_multiplier}):")
print(f"   Q1: {iqr_sales_result['q1']}, Q3: {iqr_sales_result['q3']}, IQR: {iqr_sales_result['iqr']}")
print(f"   Bounds: [{iqr_sales_result['lower_bound']}, {iqr_sales_result['upper_bound']}]")
print(f"   Anomalies: {iqr_sales_result['anomaly_count']} ({iqr_sales_result['anomaly_pct']}%)")
print(f"   Passed: {iqr_sales_result['passed']}")

# Check units_sold with IQR (multiplier from config: 1.5)
iqr_units_multiplier = next(c for c in anomaly_config if c["method"] == "iqr" and c["column"] == "units_sold")["multiplier"]
iqr_units_result = check_iqr_anomaly(sales_df, "units_sold", iqr_units_multiplier)
print(f"\n📊 IQR on 'units_sold' (multiplier={iqr_units_multiplier}):")
print(f"   Q1: {iqr_units_result['q1']}, Q3: {iqr_units_result['q3']}, IQR: {iqr_units_result['iqr']}")
print(f"   Bounds: [{iqr_units_result['lower_bound']}, {iqr_units_result['upper_bound']}]")
print(f"   Anomalies: {iqr_units_result['anomaly_count']} ({iqr_units_result['anomaly_pct']}%)")
print(f"   Passed: {iqr_units_result['passed']}")

# ============================================================================
# 7. RETRIEVE ANOMALOUS ROWS
# ============================================================================
print("\n" + "=" * 70)
print("7. RETRIEVE ANOMALOUS ROWS")
print("=" * 70)

# Get rows flagged by Z-score on sales_amount
print("\n📋 Rows flagged by Z-score on 'sales_amount':")
z_anomaly_rows = get_anomaly_rows(sales_df, "sales_amount", method="z_score", threshold=z_sales_threshold)
z_anomaly_rows.display()

# Get rows flagged by IQR on sales_amount
print("\n📋 Rows flagged by IQR on 'sales_amount':")
iqr_anomaly_rows = get_anomaly_rows(sales_df, "sales_amount", method="iqr", multiplier=iqr_sales_multiplier)
iqr_anomaly_rows.display()

print("\n💡 Teaching Point: Notice how IQR catches sales_amount=2 that Z-score misses!")
print("   The extreme value 5000 inflates the mean & stddev, masking other outliers.")
print("   IQR uses percentiles (Q1, Q3) which are robust to extreme values.")

# ============================================================================
# 8. BATCH ANOMALY DETECTION (from config)
# ============================================================================
print("\n" + "=" * 70)
print("8. BATCH ANOMALY DETECTION (from config.yaml)")
print("=" * 70)

batch_results = check_all_anomalies(sales_df, anomaly_config)

print(f"\n📋 Batch Results ({len(batch_results)} checks):")
for r in batch_results:
    method = r['check']
    column = r['column']
    anomaly_count = r['anomaly_count']
    passed = r['passed']
    icon = '✅' if passed else '❌'
    print(f"   {icon} {method} on '{column}': {anomaly_count} anomalies, passed={passed}")

# COMMAND ----------

# DBTITLE 1,Save Results to Delta Table
# ============================================================================
# 9. SAVE RESULTS TO DELTA TABLE
# ============================================================================
# Convert the batch anomaly detection results to a DataFrame
# This allows us to track and analyze anomalies over time
result_data = []
for r in batch_results:
    row = {
        "check": r["check"],
        "column": r["column"],
        "total_rows": r["total_rows"],
        "anomaly_count": r["anomaly_count"],
        "anomaly_pct": r["anomaly_pct"],
        "passed": r["passed"],
    }
    # Add method-specific fields
    if r["check"] == "z_score_anomaly":
        row["mean"] = r["mean"]
        row["stddev"] = r["stddev"]
        row["threshold"] = r["threshold"]
    elif r["check"] == "iqr_anomaly":
        row["q1"] = r["q1"]
        row["q3"] = r["q3"]
        row["iqr"] = r["iqr"]
        row["lower_bound"] = r["lower_bound"]
        row["upper_bound"] = r["upper_bound"]
    result_data.append(row)

results_df = spark.createDataFrame(result_data)

# Display the results DataFrame before saving
print("📋 Anomaly Detection Results Summary:")
results_df.display()

# Save results to Delta table for tracking and reporting
# Table: workspace.default.anomaly_report
results_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.anomaly_report")

print("\n✅ Results saved to 'workspace.default.anomaly_report' table")

# COMMAND ----------

# DBTITLE 1,Read Back Anomaly Report
# ============================================================================
# 10. VERIFY SAVED RESULTS
# ============================================================================
# Read back the saved anomaly report from Delta table to verify persistence
df = spark.table("workspace.default.anomaly_report")

print("📊 Reading back from Delta table: workspace.default.anomaly_report")
df.display()

# COMMAND ----------


