# API Reference — Reusable Methods

All functions live in `src/checks/dq_checks.py`. Import once, call from anywhere.

```python
# In a Databricks notebook:
# %run /Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/src/checks/dq_checks
```

---

## Layer 3 — Orchestrator (Automated)

### `run_all_checks(table_registry, config)`

**Purpose:** Single entry point. Reads per-dataset config, runs all declared checks across all datasets, runs cross-table FK checks, calculates DQ score. One call does everything.

**Returns:** List of result dicts (each with `passed` boolean + `dataset` key), plus a `dq_score` dict if scoring config is present.

**Example:**
```python
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

table_registry = {
    "orders": orders_df,
    "customers": customers_df,
    "employees": employees_df,
}

results = run_all_checks(table_registry, config)

# Filter to failures
failed = [r for r in results if not r.get("passed", True)]
```

---

## Layer 2 — Batch Functions (Config-Driven, Per Dimension)

Each runs one check type across multiple columns/tables from config.

### `check_completeness_all(df, max_null_pct=0)`

**Purpose:** Run completeness check across every column in a DataFrame.

**Returns:** List of dicts (one per column).

**Example:**
```python
results = check_completeness_all(orders_df, max_null_pct=5)
```

---

### `check_all_consistency(df, rules_config)`

**Purpose:** Run consistency checks for every rule declared in config. Also used for business rules.

**Returns:** List of dicts (one per rule).

**Example:**
```python
rules = [
    {"name": "ship_after_order", "expression": "ship_date >= order_date"},
    {"name": "positive_amount", "expression": "amount > 0"},
]
results = check_all_consistency(orders_df, rules)
```

---

### `check_all_foreign_keys(table_registry, fk_config)`

**Purpose:** Run referential integrity checks for every FK relationship declared in config. Supports single-column and composite keys.

**Returns:** List of dicts (one per FK relationship).

**Example:**
```python
fk_config = [
    {"child_table": "orders", "child_key": "customer_id",
     "parent_table": "customers", "parent_key": "customer_id", "composite": False},
]
results = check_all_foreign_keys(table_registry, fk_config)
```

---

### `check_all_schemas(table_registry, schema_config)`

**Purpose:** Run schema validation for every table declared in schema config.

**Returns:** List of dicts (one per table).

**Example:**
```python
schema_config = {
    "employees": [["employee_id", "integer"], ["name", "string"], ["manager_id", "double"]],
}
results = check_all_schemas(table_registry, schema_config)
```

---

### `check_all_freshness(table_registry, freshness_config)`

**Purpose:** Run freshness checks for every table declared in config.

**Returns:** List of dicts (one per table).

**Example:**
```python
freshness_config = [
    {"table": "orders", "timestamp_column": "created_at", "max_age_hours": 24},
]
results = check_all_freshness(table_registry, freshness_config)
```

---

### `check_all_reconciliation(source_df, target_df, reconciliation_config)`

**Purpose:** Run all reconciliation checks (record count, sum, row-level) declared in config.

**Returns:** List of dicts (one per check type).

**Example:**
```python
recon_config = [
    {"check": "record_count"},
    {"check": "sum", "column": "total"},
    {"check": "row_level", "key_columns": ["order_id"]},
]
results = check_all_reconciliation(source_df, target_df, recon_config)
```

---

### `check_all_anomalies(df, anomaly_config)`

**Purpose:** Run anomaly detection (z-score, IQR) for every column/method declared in config.

**Returns:** List of dicts (one per check).

**Example:**
```python
anomaly_config = [
    {"method": "z_score", "column": "sales_amount", "threshold": 3.0},
    {"method": "iqr", "column": "units_sold", "multiplier": 1.5},
]
results = check_all_anomalies(orders_df, anomaly_config)
```

---

## Layer 1 — Individual Check Functions (Manual, Granular)

### Completeness

#### `check_completeness(df, column_name, max_null_pct=0)`

**Purpose:** Check what percentage of a column's values are null.

**Returns:** Dict with `check`, `column`, `total_rows`, `null_count`, `null_pct`, `passed`.

**Example:**
```python
result = check_completeness(orders_df, "customer_id", max_null_pct=5)
# {"check": "completeness", "column": "customer_id", "total_rows": 100,
#  "null_count": 2, "null_pct": 2.0, "passed": True}
```

---

### Uniqueness

#### `check_uniqueness(df, column_name)`

**Purpose:** Check whether a column contains duplicate values.

**Returns:** Dict with `check`, `column`, `total_rows`, `distinct_count`, `duplicate_count`, `passed`.

**Example:**
```python
result = check_uniqueness(orders_df, "order_id")
```

#### `check_uniqueness_composite(df, columns)`

**Purpose:** Check uniqueness across a combination of columns (composite key).

**Returns:** Dict (same shape as `check_uniqueness`).

**Example:**
```python
result = check_uniqueness_composite(order_items_df, ["order_id", "product_id"])
```

#### `get_duplicate_rows(df, column_name)`

**Purpose:** Return the actual duplicate rows for investigation. Returns a DataFrame, not a dict.

**Returns:** DataFrame containing only the extra (non-first) duplicate rows.

**Example:**
```python
dupes = get_duplicate_rows(orders_df, "order_id")
dupes.show()
```

---

### Referential Integrity

#### `check_referential_integrity(child_df, child_key, parent_df, parent_key)`

**Purpose:** Check whether every value in child_df's FK column exists in parent_df.

**Returns:** Dict with `check`, `child_key`, `total_rows`, `orphan_count`, `passed`.

**Example:**
```python
result = check_referential_integrity(orders_df, "customer_id", customers_df, "customer_id")
```

#### `check_referential_integrity_composite(child_df, child_keys, parent_df, parent_keys)`

**Purpose:** Check referential integrity across a combination of columns (composite FK).

**Returns:** Dict (same shape as `check_referential_integrity`).

**Example:**
```python
result = check_referential_integrity_composite(
    order_items_df, ["order_id", "product_id"],
    order_products_df, ["order_id", "product_id"]
)
```

#### `get_orphaned_rows(child_df, child_key, parent_df, parent_key)`

**Purpose:** Return the actual orphaned rows from child_df for investigation. Returns a DataFrame.

**Returns:** DataFrame containing only orphan rows.

**Example:**
```python
orphans = get_orphaned_rows(orders_df, "customer_id", customers_df, "customer_id")
orphans.show()
```

---

### Schema Validation

#### `check_schema(df, expected_schema)`

**Purpose:** Compare a DataFrame's actual schema against an expected one. Detects missing columns, extra columns, and type mismatches.

**Returns:** Dict with `check`, `missing_columns`, `extra_columns`, `type_mismatches`, `passed`.

**Example:**
```python
expected = [("employee_id", "integer"), ("name", "string"), ("manager_id", "double")]
result = check_schema(employees_df, expected)
```

---

### Freshness

#### `check_freshness(df, timestamp_column, max_age_hours=24)`

**Purpose:** Check whether a table's most recent record is within the allowed age window.

**Returns:** Dict with `check`, `column`, `latest_timestamp`, `age_hours`, `passed`.

**Example:**
```python
result = check_freshness(orders_df, "created_at", max_age_hours=24)
```

---

### Consistency & Business Rules

#### `check_consistency(df, rule_name, expression)`

**Purpose:** Check how many rows violate a given SQL boolean expression. Used for both consistency rules and business rules.

**Returns:** Dict with `check`, `rule_name`, `expression`, `total_rows`, `violation_count`, `passed`.

**Example:**
```python
result = check_consistency(orders_df, "ship_after_order", "ship_date >= order_date")
```

#### `get_violating_rows(df, expression)`

**Purpose:** Return the actual rows that violate a rule for investigation. Returns a DataFrame.

**Returns:** DataFrame containing only violating rows.

**Example:**
```python
violations = get_violating_rows(orders_df, "ship_date >= order_date")
violations.show()
```

---

### Reconciliation

#### `check_record_count(source_df, target_df)`

**Purpose:** Compare row counts between source and target DataFrames.

**Returns:** Dict with `check`, `source_count`, `target_count`, `count_diff`, `passed`.

**Example:**
```python
result = check_record_count(source_df, target_df)
```

#### `check_sum_reconciliation(source_df, target_df, column_name, tolerance=0.01)`

**Purpose:** Compare the sum of a column between source and target within a tolerance.

**Returns:** Dict with `check`, `column`, `source_sum`, `target_sum`, `diff`, `passed`.

**Example:**
```python
result = check_sum_reconciliation(source_df, target_df, "total", tolerance=0.01)
```

#### `check_row_level_reconciliation(source_df, target_df, key_columns)`

**Purpose:** Compare rows between source and target using key columns. Detects missing, extra, and mismatched rows.

**Returns:** Dict with `check`, `key_columns`, `source_count`, `target_count`, `missing_count`, `extra_count`, `mismatch_count`, `passed`.

**Example:**
```python
result = check_row_level_reconciliation(source_df, target_df, ["order_id"])
```

#### `get_mismatched_rows(source_df, target_df, key_columns)`

**Purpose:** Return the actual mismatched rows from source for investigation. Returns a DataFrame.

**Returns:** DataFrame containing missing and mismatched rows.

**Example:**
```python
mismatches = get_mismatched_rows(source_df, target_df, ["order_id"])
mismatches.show()
```

---

### Anomaly Detection

#### `check_z_score_anomaly(df, column_name, threshold=3.0)`

**Purpose:** Detect anomalous values using Z-score (standard deviations from mean).

**Returns:** Dict with `check`, `column`, `mean`, `stddev`, `threshold`, `total_rows`, `anomaly_count`, `anomaly_pct`, `passed`.

**Example:**
```python
result = check_z_score_anomaly(sales_df, "sales_amount", threshold=2.0)
```

#### `check_iqr_anomaly(df, column_name, multiplier=1.5)`

**Purpose:** Detect anomalous values using the interquartile range method.

**Returns:** Dict with `check`, `column`, `q1`, `q3`, `iqr`, `lower_bound`, `upper_bound`, `total_rows`, `anomaly_count`, `anomaly_pct`, `passed`.

**Example:**
```python
result = check_iqr_anomaly(sales_df, "units_sold", multiplier=1.5)
```

#### `get_anomaly_rows(df, column_name, method="z_score", threshold=3.0, multiplier=1.5)`

**Purpose:** Return the actual anomalous rows for investigation. Returns a DataFrame.

**Returns:** DataFrame containing only anomalous rows.

**Example:**
```python
anomalies = get_anomaly_rows(sales_df, "sales_amount", method="z_score", threshold=2.0)
anomalies.show()
```

---

### DQ Scoring

#### `calculate_dq_score(check_results, scoring_config)`

**Purpose:** Calculate a weighted DQ score (0-100) from a list of check result dicts. Groups by category, applies weights, returns letter grade.

**Returns:** Dict with `check`, `total_score`, `grade`, `total_weight_applied`, `category_scores`, `passed`.

**Example:**
```python
all_results = [completeness_result, uniqueness_result, consistency_result]
scoring_config = {
    "category_mapping": {"completeness": "completeness", "uniqueness": "uniqueness"},
    "weights": {"completeness": 50, "uniqueness": 50},
    "grade_thresholds": {"A": 90, "B": 80, "C": 70, "D": 60, "F": 0},
    "min_passing_score": 80,
}
score = calculate_dq_score(all_results, scoring_config)
# {"check": "dq_score", "total_score": 100.0, "grade": "A", "passed": True}
```

---

## Quick Reference Table

| Layer | Function | Returns | Scope |
| --- | --- | --- | --- |
| 3 | `run_all_checks` | List of dicts | All checks, all datasets |
| 2 | `check_completeness_all` | List of dicts | One check type, all columns |
| 2 | `check_all_consistency` | List of dicts | One check type, all rules |
| 2 | `check_all_foreign_keys` | List of dicts | One check type, all FKs |
| 2 | `check_all_schemas` | List of dicts | One check type, all tables |
| 2 | `check_all_freshness` | List of dicts | One check type, all tables |
| 2 | `check_all_reconciliation` | List of dicts | One check type, all checks |
| 2 | `check_all_anomalies` | List of dicts | One check type, all columns |
| 1 | `check_completeness` | Dict | One column |
| 1 | `check_uniqueness` | Dict | One column |
| 1 | `check_uniqueness_composite` | Dict | Multiple columns |
| 1 | `check_referential_integrity` | Dict | One FK relationship |
| 1 | `check_referential_integrity_composite` | Dict | One composite FK |
| 1 | `check_schema` | Dict | One table |
| 1 | `check_freshness` | Dict | One table |
| 1 | `check_consistency` | Dict | One rule |
| 1 | `check_record_count` | Dict | Source vs target |
| 1 | `check_sum_reconciliation` | Dict | One column, source vs target |
| 1 | `check_row_level_reconciliation` | Dict | Key columns, source vs target |
| 1 | `check_z_score_anomaly` | Dict | One column |
| 1 | `check_iqr_anomaly` | Dict | One column |
| 1 | `calculate_dq_score` | Dict | All results |
| — | `get_duplicate_rows` | DataFrame | Investigation |
| — | `get_orphaned_rows` | DataFrame | Investigation |
| — | `get_violating_rows` | DataFrame | Investigation |
| — | `get_mismatched_rows` | DataFrame | Investigation |
| — | `get_anomaly_rows` | DataFrame | Investigation |

---

## Investigation vs Check Functions

**Check functions** (return dicts): Summarize the problem — counts, percentages, pass/fail. Use for reporting and scoring.

**Investigation functions** (return DataFrames): Return the actual bad rows. Use for debugging and root-cause analysis. Always prefixed with `get_`.

```python
# Check: is there a problem?
result = check_completeness(orders_df, "customer_id")
if not result["passed"]:
    # Investigate: which rows are bad?
    # (No get_ variant for completeness, but for uniqueness:)
    dupes = get_duplicate_rows(orders_df, "order_id")
    dupes.show()
```