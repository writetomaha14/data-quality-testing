# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Consistency Rules Demo - Overview
# MAGIC %md
# MAGIC # Day 37: Consistency Rules Check Demo
# MAGIC
# MAGIC This notebook demonstrates how to validate **business rule consistency** within a single table using the data quality framework.
# MAGIC
# MAGIC ## Overview
# MAGIC Consistency rules verify that data within a single table follows business logic constraints (e.g., ship_date >= order_date, total = quantity x unit_price). Unlike foreign keys which validate cross-table relationships, consistency rules validate intra-table business rules.
# MAGIC
# MAGIC ## What This Demo Covers
# MAGIC * Loading test data from CSV files (valid and violation datasets)
# MAGIC * Loading consistency rules from YAML configuration (config.yaml)
# MAGIC * Running single rule checks using `check_consistency()`
# MAGIC * Running batch rule checks using `check_all_consistency()`
# MAGIC * Identifying violating rows using `get_violating_rows()`
# MAGIC * Saving results to a Delta table for tracking and reporting
# MAGIC
# MAGIC ## Test Scenario
# MAGIC * **Valid Dataset**: orders_consistency_valid.csv (5 rows, all pass)
# MAGIC * **Violation Dataset**: orders_consistency_violations.csv (6 rows, some fail)
# MAGIC * **Rules**: Loaded from config.yaml → consistency_rules
# MAGIC   * `ship_after_order`: ship_date >= order_date
# MAGIC   * `total_matches_calculation`: ABS(total - (quantity * unit_price)) < 0.01
# MAGIC * **Expected Issue**: 2 rows violate ship_after_order, 2 rows violate total_matches_calculation
# MAGIC
# MAGIC ## Key Functions
# MAGIC * `check_consistency(df, rule_name, expression)` - Validates a single consistency rule
# MAGIC * `check_all_consistency(df, rules_config)` - Batch validates multiple rules
# MAGIC * `get_violating_rows(df, expression)` - Returns rows that violate a rule
# MAGIC
# MAGIC ## Output
# MAGIC * Consistency validation report showing violation counts and pass/fail status
# MAGIC * Results saved to `workspace.default.consistency_report` table

# COMMAND ----------

# DBTITLE 1,Demo: Consistency Rules Check
"""
Consistency Rules Check Demo

This script demonstrates how to validate business rule consistency within a single table.
It checks that data follows logical constraints using SQL expressions loaded from YAML config.

Consistency Rules (from config.yaml):
    1. ship_after_order: ship_date >= order_date
    2. total_matches_calculation: ABS(total - (quantity * unit_price)) < 0.01
    
Expected Results:
    - Valid dataset: All rows pass all rules
    - Violation dataset: 2 rows violate ship_after_order, 2 rows violate total_matches_calculation
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
# Go up three levels: day37 -> phase2_dq_framework -> notebooks -> data-quality-testing (base)
base_path = os.path.dirname(os.path.dirname(os.path.dirname(notebook_path)))

# ============================================================================
# 3. IMPORT DQ CHECK FUNCTIONS
# ============================================================================
# Add DQ checks module to Python path
checks_path = os.path.join("/Workspace", base_path.lstrip("/"), "src/checks")
sys.path.append(checks_path)
from dq_checks import check_consistency, check_all_consistency, get_violating_rows

# ============================================================================
# 4. LOAD CONFIGURATION
# ============================================================================
# Load YAML configuration file containing consistency rule definitions
config_path = os.path.join("/Workspace", base_path.lstrip("/"), "src/config/config.yaml")
with open(config_path) as f:
    config = yaml.safe_load(f)

consistency_rules = config["consistency_rules"]
print(f"📋 Loaded {len(consistency_rules)} consistency rules from config.yaml:")
for rule in consistency_rules:
    print(f"   - {rule['name']}: {rule['expression']}")

# ============================================================================
# 5. LOAD TEST DATA - VALID DATASET
# ============================================================================
# Load orders test data where all rows pass all consistency rules
valid_path = os.path.join("/Workspace", base_path.lstrip("/"), "tests/test_data/orders_consistency_valid.csv")
valid_orders = spark.read.option("header", True).option("inferSchema", True).csv(valid_path)

print("\n📊 Valid Orders Data (all rows should pass):")
valid_orders.display()

# ============================================================================
# 6. LOAD TEST DATA - VIOLATION DATASET
# ============================================================================
# Load orders test data with intentional consistency rule violations
violation_path = os.path.join("/Workspace", base_path.lstrip("/"), "tests/test_data/orders_consistency_violations.csv")
violation_orders = spark.read.option("header", True).option("inferSchema", True).csv(violation_path)

print("\n📊 Violation Orders Data (some rows should fail):")
violation_orders.display()

# ============================================================================
# 7. RUN SINGLE RULE CHECK ON VALID DATA
# ============================================================================
# Check ship_after_order rule on valid dataset (should pass)
ship_rule = next(r for r in consistency_rules if r["name"] == "ship_after_order")
print(f"\n🔍 Checking rule '{ship_rule['name']}' on VALID data...")
valid_result = check_consistency(valid_orders, ship_rule["name"], ship_rule["expression"])
print(f"📊 Result: {valid_result}")

# ============================================================================
# 8. RUN SINGLE RULE CHECK ON VIOLATION DATA
# ============================================================================
# Check ship_after_order rule on violation dataset (should find violations)
print(f"\n🔍 Checking rule '{ship_rule['name']}' on VIOLATION data...")
violation_result = check_consistency(violation_orders, ship_rule["name"], ship_rule["expression"])
print(f"📊 Result: {violation_result}")

# ============================================================================
# 9. IDENTIFY VIOLATING ROWS
# ============================================================================
# Get all rows that violate the ship_after_order rule
print("\n🚨 Violating Rows (ship_date < order_date):")
violating_rows = get_violating_rows(violation_orders, ship_rule["expression"])
violating_rows.display()

# ============================================================================
# 10. RUN BATCH CHECK ON VIOLATION DATA
# ============================================================================
# Run all consistency rules from config against the violation dataset
print(f"\n🔍 Running all {len(consistency_rules)} consistency rules on VIOLATION data...")
batch_results = check_all_consistency(violation_orders, consistency_rules)

for r in batch_results:
    print(f"   📊 {r['rule_name']}: violations={r['violation_count']}, passed={r['passed']}")

# COMMAND ----------

# DBTITLE 1,Save Results to Delta Table
# ============================================================================
# 11. SAVE RESULTS TO DELTA TABLE
# ============================================================================
# Convert the batch consistency check results to a DataFrame
# This allows us to track and analyze consistency violations over time
result_data = [{
    "check": r["check"],
    "rule_name": r["rule_name"],
    "expression": r["expression"],
    "total_rows": r["total_rows"],
    "violation_count": r["violation_count"],
    "passed": r["passed"]
} for r in batch_results]

results_df = spark.createDataFrame(result_data)

# Display the results DataFrame before saving
print("\n📋 Consistency Check Results Summary:")
results_df.display()

# Save results to Delta table for tracking and reporting
# Table: workspace.default.consistency_report
results_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.consistency_report")

print("\n✅ Results saved to 'workspace.default.consistency_report' table")

# COMMAND ----------

# DBTITLE 1,Read Back Consistency Report
# ============================================================================
# 12. VERIFY SAVED RESULTS
# ============================================================================
# Read back the saved consistency report from Delta table to verify persistence
df = spark.table("workspace.default.consistency_report")

print("📊 Reading back from Delta table: workspace.default.consistency_report")
df.display()

# COMMAND ----------


