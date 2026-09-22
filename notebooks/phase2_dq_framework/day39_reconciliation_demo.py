# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Reconciliation Demo - Overview
# MAGIC %md
# MAGIC # Day 39: Reconciliation Check Demo
# MAGIC
# MAGIC This notebook demonstrates how to compare two datasets (source vs target) to verify they match across record counts, aggregated sums, and row-level values.
# MAGIC
# MAGIC ## Overview
# MAGIC Reconciliation answers: "Did all the data arrive correctly from source to target?" It compares a source dataset against a target dataset to detect data loss, corruption, or drift during pipeline execution.
# MAGIC
# MAGIC ## What This Demo Covers
# MAGIC * Loading source and target datasets from CSV files
# MAGIC * Loading reconciliation rules from YAML configuration (config.yaml)
# MAGIC * Comparing record counts using `check_record_count()`
# MAGIC * Comparing column sums using `check_sum_reconciliation()`
# MAGIC * Comparing rows at the key level using `check_row_level_reconciliation()`
# MAGIC * Retrieving actual mismatched rows using `get_mismatched_rows()`
# MAGIC * Running all checks in batch using `check_all_reconciliation()`
# MAGIC * Saving results to a Delta table for tracking and reporting
# MAGIC
# MAGIC ## Test Scenario
# MAGIC * **Source Dataset**: reconciliation_source.csv (5 rows)
# MAGIC * **Target Dataset (matching)**: reconciliation_target.csv (identical copy — all checks pass)
# MAGIC * **Mismatch Target**: reconciliation_mismatch_target.csv (4 rows with differences)
# MAGIC   * 2 rows missing from target (order_id 1003, 1005)
# MAGIC   * 1 extra row in target (order_id 1006)
# MAGIC   * 1 value mismatch (order_id 1004: total changed from 300 to 350)
# MAGIC * **Rules**: Loaded from config.yaml → reconciliation
# MAGIC   * `record_count`: Compare total row counts
# MAGIC   * `sum`: Compare sum of `total` column
# MAGIC   * `row_level`: Compare rows using `order_id` as key
# MAGIC
# MAGIC ## Key Functions
# MAGIC * `check_record_count(source_df, target_df)` - Compare row counts
# MAGIC * `check_sum_reconciliation(source_df, target_df, column_name)` - Compare column sums
# MAGIC * `check_row_level_reconciliation(source_df, target_df, key_columns)` - Compare rows by key
# MAGIC * `get_mismatched_rows(source_df, target_df, key_columns)` - Get actual mismatched rows
# MAGIC * `check_all_reconciliation(source_df, target_df, reconciliation_config)` - Run all checks from config

# COMMAND ----------

# DBTITLE 1,Demo: Reconciliation Check
"""
Reconciliation Check Demo

This script demonstrates how to compare source and target datasets to verify
data arrived correctly. It uses reconciliation functions from dq_checks.py
and loads all configuration from config.yaml.

Reconciliation Checks (from config.yaml):
    1. record_count: Compare total row counts between source and target
    2. sum: Compare sum of the 'total' column
    3. row_level: Compare rows using 'order_id' as key
    
Expected Results:
    - Matching datasets: All checks pass
    - Mismatch dataset: Record count differs, sum differs, rows missing/extra/mismatched
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
# Go up three levels: day39 -> phase2_dq_framework -> notebooks -> data-quality-testing (base)
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

# Import reconciliation functions
from dq_checks import (
    check_record_count,
    check_sum_reconciliation,
    check_row_level_reconciliation,
    check_all_reconciliation,
    get_mismatched_rows,
)

print("✅ All reconciliation functions imported successfully")

# ============================================================================
# 3. LOAD CONFIGURATION FROM YAML
# ============================================================================
config_path = os.path.join(CONFIG_DIR, "config.yaml")
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

reconciliation_rules = config.get('reconciliation', [])

print(f"\n📋 Loaded {len(reconciliation_rules)} reconciliation rules from config.yaml:")
for rule in reconciliation_rules:
    print(f"   - {rule['check']}: {rule.get('column', rule.get('key_columns', ''))}")

# Extract config parameters (no hard-coded values in the demo)
sum_column = next(r for r in reconciliation_rules if r["check"] == "sum")["column"]
row_key_columns = next(r for r in reconciliation_rules if r["check"] == "row_level")["key_columns"]

# ============================================================================
# 4. LOAD TEST DATA
# ============================================================================
print("\n" + "=" * 70)
print("Loading test data...")
print("=" * 70)

# Source dataset
source_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(os.path.join(TEST_DATA_DIR, "reconciliation_source.csv"))

# Target dataset (identical to source — all checks should pass)
target_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(os.path.join(TEST_DATA_DIR, "reconciliation_target.csv"))

# Mismatch target (intentional differences)
mismatch_target_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(os.path.join(TEST_DATA_DIR, "reconciliation_mismatch_target.csv"))

print(f"\n📊 Source Data ({source_df.count()} rows):")
source_df.display()

print(f"\n📊 Target Data — matching ({target_df.count()} rows):")
target_df.display()

print(f"\n📊 Mismatch Target Data ({mismatch_target_df.count()} rows):")
mismatch_target_df.display()

# ============================================================================
# 5. RECORD COUNT RECONCILIATION
# ============================================================================
print("\n" + "=" * 70)
print("1️⃣  Record Count Reconciliation")
print("=" * 70)

# Matching datasets
result = check_record_count(source_df, target_df)
print(f"\n✅ Matching datasets:")
print(f"   Source: {result['source_count']} rows | Target: {result['target_count']} rows")
print(f"   Count diff: {result['count_diff']} | Passed: {result['passed']}")

# Mismatch datasets
result = check_record_count(source_df, mismatch_target_df)
print(f"\n⚠️  Mismatch datasets:")
print(f"   Source: {result['source_count']} rows | Target: {result['target_count']} rows")
print(f"   Count diff: {result['count_diff']} | Passed: {result['passed']}")

# ============================================================================
# 6. SUM RECONCILIATION
# ============================================================================
print("\n" + "=" * 70)
print(f"2️⃣  Sum Reconciliation (column: {sum_column})")
print("=" * 70)

# Matching datasets
result = check_sum_reconciliation(source_df, target_df, sum_column)
print(f"\n✅ Matching datasets:")
print(f"   Source sum: {result['source_sum']} | Target sum: {result['target_sum']}")
print(f"   Diff: {result['diff']} | Passed: {result['passed']}")

# Mismatch datasets
result = check_sum_reconciliation(source_df, mismatch_target_df, sum_column)
print(f"\n⚠️  Mismatch datasets:")
print(f"   Source sum: {result['source_sum']} | Target sum: {result['target_sum']}")
print(f"   Diff: {result['diff']} | Passed: {result['passed']}")

# ============================================================================
# 7. ROW-LEVEL RECONCILIATION
# ============================================================================
print("\n" + "=" * 70)
print(f"3️⃣  Row-Level Reconciliation (key: {row_key_columns})")
print("=" * 70)

# Matching datasets
result = check_row_level_reconciliation(source_df, target_df, row_key_columns)
print(f"\n✅ Matching datasets:")
print(f"   Missing: {result['missing_count']} | Extra: {result['extra_count']} | Mismatch: {result['mismatch_count']}")
print(f"   Passed: {result['passed']}")

# Mismatch datasets
result = check_row_level_reconciliation(source_df, mismatch_target_df, row_key_columns)
print(f"\n⚠️  Mismatch datasets:")
print(f"   Missing: {result['missing_count']} | Extra: {result['extra_count']} | Mismatch: {result['mismatch_count']}")
print(f"   Passed: {result['passed']}")

# Show actual mismatched rows
print(f"\n🔍 Actual mismatched rows from source:")
mismatched = get_mismatched_rows(source_df, mismatch_target_df, row_key_columns)
mismatched.display()

# ============================================================================
# 8. BATCH RECONCILIATION (ALL CHECKS FROM CONFIG)
# ============================================================================
print("\n" + "=" * 70)
print("4️⃣  Batch Reconciliation (all checks from config.yaml)")
print("=" * 70)

# Run all checks on matching data
print("\n✅ Matching datasets:")
batch_results = check_all_reconciliation(source_df, target_df, reconciliation_rules)
for r in batch_results:
    print(f"   {r['check']}: passed={r['passed']}")

# Run all checks on mismatch data
print("\n⚠️  Mismatch datasets:")
batch_results = check_all_reconciliation(source_df, mismatch_target_df, reconciliation_rules)
for r in batch_results:
    status = "✅" if r["passed"] else "❌"
    print(f"   {status} {r['check']}: passed={r['passed']}")

print("\n" + "=" * 70)
print("Reconciliation demo complete!")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Save Results to Delta Table
# ============================================================================
# 9. SAVE RESULTS TO DELTA TABLE
# ============================================================================
# Convert the batch reconciliation results to a DataFrame
# This allows us to track and analyze reconciliation issues over time
result_data = []
for r in batch_results:
    row = {
        "check": r["check"],
        "passed": r["passed"],
    }
    # Add check-specific fields
    if r["check"] == "record_count_reconciliation":
        row["source_count"] = r["source_count"]
        row["target_count"] = r["target_count"]
        row["count_diff"] = r["count_diff"]
    elif r["check"] == "sum_reconciliation":
        row["column"] = r["column"]
        row["source_sum"] = r["source_sum"]
        row["target_sum"] = r["target_sum"]
        row["diff"] = r["diff"]
    elif r["check"] == "row_level_reconciliation":
        row["missing_count"] = r["missing_count"]
        row["extra_count"] = r["extra_count"]
        row["mismatch_count"] = r["mismatch_count"]
    result_data.append(row)

results_df = spark.createDataFrame(result_data)

# Display the results DataFrame before saving
print("📋 Reconciliation Check Results Summary:")
results_df.display()

# Save results to Delta table for tracking and reporting
# Table: workspace.default.reconciliation_report
results_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.reconciliation_report")

print("\n✅ Results saved to 'workspace.default.reconciliation_report' table")

# COMMAND ----------

# DBTITLE 1,Read Back Reconciliation Report
# ============================================================================
# 10. VERIFY SAVED RESULTS
# ============================================================================
# Read back the saved reconciliation report from Delta table to verify persistence
df = spark.table("workspace.default.reconciliation_report")

print("📊 Reading back from Delta table: workspace.default.reconciliation_report")
df.display()

# COMMAND ----------


