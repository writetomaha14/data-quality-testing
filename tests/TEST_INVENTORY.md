# Data Quality Testing Framework — Test Inventory

**Project:** Data Quality Engineering Initiative  
**Repository:** data-quality-testing  
**Platform:** Databricks (PySpark, pytest)  
**Last Updated:** 2024

---

## Test Suites Overview

| # | Test Suite | Test File | Tests | Status |
| --- | --- | --- | --- | --- |
| 1 | Consistency Rules | test_consistency_rules.py | 6 | ✅ Complete |
| 2 | Referential Integrity | test_referential_integrity.py | 4 | ✅ Complete |
| 3 | Foreign Keys (Composite) | test_foreign_keys.py | 3 | ✅ Complete |
| 4 | Schema Validation | test_schema_validation.py | 1 | ✅ Complete |
| 5 | Completeness | test_completeness.py | 3 | ✅ Complete |
| 6 | Uniqueness | test_uniqueness.py | 4 | ✅ Complete |
| 7 | Freshness | test_freshness.py | 7 | ✅ Complete |
| | **TOTAL** | | **28** | |

---

## 1. Consistency Rules — Test Inventory

**Test File:** test_consistency_rules.py  
**Config Source:** config.yaml → consistency_rules  
**DQ Functions Tested:** check_consistency, check_all_consistency, get_violating_rows

### Test Data Files

| File | Location | Purpose | Rows |
| --- | --- | --- | --- |
| orders_consistency_valid.csv | tests/test_data/ | All rows pass all rules | 5 |
| orders_consistency_violations.csv | tests/test_data/ | Intentional rule violations | 6 |
| config.yaml | src/config/ | Consistency rule definitions (YAML) | 2 rules |

### Test Cases

| # | Test Name | Description | Category | Test Data | Rule | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | test_consistency_single_rule_passes | Verify check passes when all rows satisfy ship_after_order rule | Pass (Happy Path) | orders_consistency_valid.csv | ship_after_order | Date comparison rule loaded from YAML config |
| 2 | test_consistency_total_calculation_rule_passes | Verify check passes when total = quantity x unit_price for all rows | Pass (Happy Path) | orders_consistency_valid.csv | total_matches_calculation | Numeric calculation rule with ABS tolerance < 0.01 |
| 3 | test_check_all_consistency_from_yaml_config | Verify batch checking runs all rules from YAML and all pass on valid data | Pass (Batch) | orders_consistency_valid.csv | All rules | Tests check_all_consistency function with YAML-driven config |
| 4 | test_consistency_finds_ship_date_violations | Verify check detects 2 rows where ship_date < order_date | Fail (Violation Detection) | orders_consistency_violations.csv | ship_after_order | Rows 1002, 1006 violate ship_date >= order_date |
| 5 | test_consistency_finds_total_calculation_violations | Verify check detects 2 rows where total != quantity x unit_price | Fail (Violation Detection) | orders_consistency_violations.csv | total_matches_calculation | Rows 1004, 1006 violate total calculation |
| 6 | test_get_violating_rows_returns_correct_rows | Verify helper function returns rows matching violation count from check function | Helper Function | orders_consistency_violations.csv | ship_after_order | Cross-validates check_consistency vs get_violating_rows |

### YAML Rule Configuration

| Rule Name | Expression | Type |
| --- | --- | --- |
| ship_after_order | `ship_date >= order_date` | Date comparison |
| total_matches_calculation | `ABS(total - (quantity * unit_price)) < 0.01` | Numeric calculation |

### Fixtures Used

| Fixture | Source | Scope | Description |
| --- | --- | --- | --- |
| consistency_test_data | conftest.py | module | Loads orders_consistency_valid.csv |
| consistency_violation_data | conftest.py | module | Loads orders_consistency_violations.csv |
| consistency_rules | conftest.py | module | Loads consistency_rules from config.yaml |
| spark_session | conftest.py | session | Shared SparkSession |

---

## 2. Referential Integrity — Test Inventory

**Test File:** test_referential_integrity.py  
**DQ Functions Tested:** check_referential_integrity, get_orphaned_rows

### Test Data Files

| File | Location | Purpose |
| --- | --- | --- |
| customers.csv | tests/test_data/ | Parent table with customer records |
| orders.csv | tests/test_data/ | Child table with order records (includes orphans) |

### Test Cases

| # | Test Name | Description | Category | Test Data | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | test_referential_integrity_finds_orphan | Detects orphaned records when they exist | Fail (Violation Detection) | orders.csv, customers.csv | Verifies orphan_count > 0 and passed = False |
| 2 | test_referential_integrity_clean_relationship | Passes when no orphans exist | Pass (Happy Path) | Inline (clean_orders) | Uses inline DataFrame with valid FK references |
| 3 | test_get_orphaned_rows_returns_correct_rows | Helper returns matching orphan count from check function | Helper Function | orders.csv, customers.csv | Cross-validates check vs get function |
| 4 | test_null_foreign_key_documented_behavior | Null FK treated as orphan | Edge Case | Inline (orders_with_null) | Documents expected behavior for null FK handling |

---

## 3. Foreign Keys (Composite) — Test Inventory

**Test File:** test_foreign_keys.py  
**Config Source:** config.yaml → foreign_keys  
**DQ Functions Tested:** check_referential_integrity, check_all_foreign_keys, check_referential_integrity_composite

### Test Data Files

| File | Location | Purpose |
| --- | --- | --- |
| orders_fk.csv | tests/test_data/ | Child table for FK relationship tests |
| customers.csv | tests/test_data/ | Parent table for simple FK |
| order_line_items.csv | tests/test_data/ | Child table for composite FK |
| order_products.csv | tests/test_data/ | Parent table for composite FK |

### Test Cases

| # | Test Name | Description | Category | Test Data | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | test_orders_to_customers_fk | Test simple FK: orders.customer_id -> customers.customer_id | Pass + Fail | orders_fk.csv, customers.csv | Validates orphan detection for single-column FK |
| 2 | test_line_items_to_products_composite_fk | Test composite FK: [order_id, product_id] | Pass + Fail | order_line_items.csv, order_products.csv | Validates composite key FK relationship |
| 3 | test_check_all_foreign_keys_integration | Integration test: run all FK checks from config | Integration | All FK tables via config | Batch validation using config.yaml foreign_keys |

---

## 4. Schema Validation — Test Inventory

**Test File:** test_schema_validation.py  
**Config Source:** config.yaml → expected_schemas  
**DQ Functions Tested:** check_schema

### Test Data Files

| File | Location | Purpose |
| --- | --- | --- |
| employees_hierarchy.csv | tests/test_data/ | Base table for schema matching |
| employees_missing_cols.csv | tests/test_data/ | Table with missing columns |
| employees_extra_cols.csv | tests/test_data/ | Table with extra columns |
| employees_wrong_types.csv | tests/test_data/ | Table with wrong data types |

### Test Cases

| # | Test Name | Description | Category | Test Data | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | test_exact_schema_match | Verify schema validation passes when columns and types match exactly | Pass (Happy Path) | employees_hierarchy.csv | Validates against expected_schemas in config.yaml |

---

## 5. Completeness — Test Inventory

**Test File:** test_completeness.py  
**DQ Functions Tested:** check_completeness, check_completeness_all

### Test Data Files

| File | Location | Purpose |
| --- | --- | --- |
| employees_hierarchy.csv | tests/test_data/ | Employee data with some null salary values |

### Test Cases

| # | Test Name | Description | Category | Test Data | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | test_completeness_salary_has_nulls | Test that check fails when null percentage exceeds threshold | Fail (Violation Detection) | employees_hierarchy.csv | Validates null detection in salary column |
| 2 | test_completeness_id_has_no_nulls | Test that check passes when column has no null values | Pass (Happy Path) | employees_hierarchy.csv | Validates clean column passes completeness |
| 3 | test_completeness_all_covers_every_column | Test that check_completeness_all returns results for all columns | Batch | employees_hierarchy.csv | Validates batch function covers every column in DataFrame |

---

## 6. Uniqueness — Test Inventory

**Test File:** test_uniqueness.py  
**DQ Functions Tested:** check_uniqueness, get_duplicate_rows

### Test Data Files

| File | Location | Purpose |
| --- | --- | --- |
| employees_hierarchy.csv | tests/test_data/ | Employee data with potential duplicates |

### Test Cases

| # | Test Name | Description | Category | Test Data | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | test_uniqueness_id_has_duplicate | Validates check_uniqueness identifies duplicate values in a column | Fail (Violation Detection) | employees_hierarchy.csv | Tests single-column duplicate detection |
| 2 | test_uniqueness_name_no_department_no_duplicates_check | Validates handling of nulls and duplicates in name/department columns | Edge Case | employees_hierarchy.csv | Tests null value handling in uniqueness checks |
| 3 | test_get_duplicate_rows_returns_only_extras | Validates get_duplicate_rows returns only extra occurrences, not first | Helper Function | employees_hierarchy.csv | Cross-validates check vs get function |
| 4 | test_uniqueness_composite_id_and_name | Validates uniqueness check across multiple columns (composite key) | Pass + Fail | employees_hierarchy.csv | Tests composite key uniqueness validation |

---

## 7. Freshness — Test Inventory

**Test File:** test_freshness.py  
**DQ Functions Tested:** check_freshness, check_all_freshness

### Test Data Files

| File | Location | Purpose |
| --- | --- | --- |
| orders_with_timestamps.csv | tests/test_data/ | Orders with timestamp columns for freshness checks |

### Test Cases

| # | Test Name | Description | Category | Test Data | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | test_freshness_data_is_fresh | Test that check passes when data is fresh (within threshold) | Pass (Happy Path) | orders_with_timestamps.csv | Validates recent timestamps pass |
| 2 | test_freshness_data_is_stale | Test that check fails when data is stale (exceeds threshold) | Fail (Violation Detection) | orders_with_timestamps.csv | Validates old timestamps are detected |
| 3 | test_freshness_with_custom_threshold | Test freshness check with custom threshold (1 hour) | Pass (Custom Config) | orders_with_timestamps.csv | Validates configurable threshold parameter |
| 4 | test_freshness_with_null_timestamps | Test that check fails gracefully when all timestamps are null | Edge Case | Inline (null timestamps) | Documents behavior for null timestamp handling |
| 5 | test_freshness_with_empty_dataframe | Test that check handles empty DataFrames | Edge Case | Inline (empty DF) | Documents behavior for empty DataFrame edge case |
| 6 | test_check_all_freshness | Test batch freshness check across multiple tables | Batch | orders_with_timestamps.csv | Validates check_all_freshness function |
| 7 | test_freshness_boundary_condition | Test freshness check at exact threshold boundary | Edge Case | orders_with_timestamps.csv | Validates behavior at exact threshold limit |

---

## 8. Business Rules — Test Inventory

**Test File:** test_business_rules.py  
**Config Source:** config.yaml → business_rules  
**DQ Functions Tested:** check_consistency, check_all_consistency, get_violating_rows

### Test Data Files

| File | Location | Purpose | Rows |
| --- | --- | --- | --- |
| orders_business_valid.csv | tests/test_data/ | All rows pass all rules | 5 |
| orders_business_violations.csv | tests/test_data/ | Intentional rule violations | 6 |
| config.yaml | src/config/ | Business rule definitions (YAML) | 2 rules |

### Test Cases

| # | Test Name | Description | Category | Test Data | Rule | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | test_business_single_rule_passes | Verify check passes when all status values are in allowed set | Pass (Happy Path) | orders_business_valid.csv | valid_status_enum | Enum / allowed-values rule loaded from YAML config |
| 2 | test_business_discount_rule_passes | Verify check passes when discount <= total for all rows | Pass (Happy Path) | orders_business_valid.csv | discount_within_total | Business logic constraint rule |
| 3 | test_check_all_business_rules_from_yaml_config | Verify batch checking runs all rules from YAML and all pass on valid data | Pass (Batch) | orders_business_valid.csv | All rules | Tests check_all_consistency function with YAML-driven config |
| 4 | test_business_finds_status_violations | Verify check detects 2 rows with invalid status values | Fail (Violation Detection) | orders_business_violations.csv | valid_status_enum | Rows 1005, 1006 violate status enum (returned, cancelled) |
| 5 | test_business_finds_discount_violations | Verify check detects 2 rows where discount > total | Fail (Violation Detection) | orders_business_violations.csv | discount_within_total | Rows 1004, 1005 violate discount <= total |
| 6 | test_get_violating_rows_for_business_rules | Verify helper function returns rows matching violation count from check function | Helper Function | orders_business_violations.csv | valid_status_enum | Cross-validates check_consistency vs get_violating_rows |

### YAML Rule Configuration

| Rule Name | Expression | Type |
| --- | --- | --- |
| valid_status_enum | `status IN ('active', 'shipped', 'pending', 'closed')` | Enum / allowed values |
| discount_within_total | `discount <= total` | Business logic constraint |

### Fixtures Used

| Fixture | Source | Scope | Description |
| --- | --- | --- | --- |
| business_test_data | conftest.py | module | Loads orders_business_valid.csv |
| business_violation_data | conftest.py | module | Loads orders_business_violations.csv |
| business_rules | conftest.py | module | Loads business_rules from config.yaml |
| spark_session | conftest.py | session | Shared SparkSession |

---

## Test Data Directory

All test data files are located in `tests/test_data/`:

| File | Used By | Description |
| --- | --- | --- |
| customers.csv | Referential Integrity | Parent table (3 customers) |
| orders.csv | Referential Integrity | Child table with orphaned orders |
| orders_fk.csv | Foreign Keys | Orders for FK relationship tests |
| order_line_items.csv | Foreign Keys | Line items for composite FK tests |
| order_products.csv | Foreign Keys | Products for composite FK tests |
| orders_consistency_valid.csv | Consistency Rules | Valid orders (5 rows, all rules pass) |
| orders_consistency_violations.csv | Consistency Rules | Orders with violations (6 rows, 2 rules broken) |
| employees_hierarchy.csv | Schema, Referential Integrity | Employee hierarchy with self-referencing FK |
| departments_hierarchy.csv | Referential Integrity | Departments hierarchy |
| employees_extra_cols.csv | Schema Validation | Employees with extra columns |
| employees_missing_cols.csv | Schema Validation | Employees with missing columns |
| employees_wrong_types.csv | Schema Validation | Employees with wrong data types |
| orders_with_timestamps.csv | Freshness | Orders with timestamp columns for freshness checks |
| orders_business_valid.csv | Business Rules | Valid orders (5 rows, all business rules pass) |
| orders_business_violations.csv | Business Rules | Orders with violations (6 rows, 2 rules broken) |

---

## Configuration Files

| File | Location | Purpose |
| --- | --- | --- |
| config.yaml | src/config/ | Central configuration (FK definitions, consistency rules, business rules, schema expectations, thresholds) |
| conftest.py | tests/ | Pytest fixtures (spark_session, test_data, config loaders) |
| pytest.ini | tests/ | Pytest configuration |
