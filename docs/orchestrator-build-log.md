# Build Log: Layer 3 Orchestrator — run_all_checks()

## Date
September 28, 2026

## Objective
Add a single entry point (`run_all_checks()`) that reads per-dataset config and automatically runs all applicable checks across all declared datasets. This completes the framework's three-layer architecture.

## What Changed

### 1. config.yaml — Per-Dataset Configuration (new top-level section)
Added a `datasets:` section where each dataset declares which checks apply and with what parameters. Only declared checks run — omit a check type to skip it for that dataset.

Current datasets declared:
- `orders`: completeness, uniqueness, consistency, business_rules, anomalies, freshness, reconciliation
- `customers`: completeness, uniqueness
- `employees`: completeness, schema

Existing global sections (`foreign_keys`, `dq_scoring`, etc.) kept as-is for backward compatibility and cross-table checks.

### 2. dq_checks.py — check_all_schemas() (new function)
Added the missing batch function for schema validation, matching the pattern of `check_all_foreign_keys()` and `check_all_freshness()`. Iterates over expected schema entries and calls `check_schema()` per table.

### 3. dq_checks.py — run_all_checks() (new function, Layer 3)
The orchestrator. Reads `config["datasets"]`, loops over each dataset, and dispatches only the declared checks by calling existing Layer 1/2 functions:
- completeness → `check_completeness()` per column
- uniqueness → `check_uniqueness()` or `check_uniqueness_composite()` per column/key
- schema → `check_schema()`
- freshness → `check_freshness()`
- consistency → `check_all_consistency()`
- business_rules → `check_all_consistency()` (same function, different rules)
- anomalies → `check_all_anomalies()`
- reconciliation → `check_all_reconciliation()` (uses target_table from registry)

After per-dataset checks:
- Runs cross-table FK checks from global `foreign_keys` config
- Calculates DQ score from all results if `dq_scoring` config is present

Returns a flat list of result dicts. Each per-dataset result includes a `dataset` key for traceability.

### 4. test_run_all_checks.py (new test file, 6 tests)
All tests pass:
- Multiple datasets return results for both
- Only declared checks run (omitted checks skipped)
- Empty datasets config returns empty results
- DQ score calculated when scoring config present (score=100, grade=A)
- No score appended when scoring config absent
- Every result has a `passed` boolean

### 5. README.md updated
- Added Layer 3 example usage
- Updated repo structure to include new files
- Moved roadmap items to completed

## Design Decisions

### Why a per-dataset config section instead of reusing global sections?
Global sections (`consistency_rules`, `anomaly_checks`, etc.) are flat — they apply to whatever DataFrame you pass. The per-dataset section lets each dataset declare which checks apply to it and with what parameters. This is what enables "configure N datasets, run everything in one call."

### Why does business_rules use check_all_consistency()?
Business rules are conceptually different from consistency rules (policy vs. logic), but mechanically they're the same: a SQL boolean expression that should be true for every row. Rather than duplicate the consistency check logic, the orchestrator calls the same function with different rule sets. The DQ score groups both under the `consistency` category.

### Why add `dataset` key to each result?
When running checks across multiple datasets, results need to be traceable back to their source. The `dataset` key enables filtering results by dataset after the orchestrator runs.

## Files Modified
- `src/config/config.yaml` — added `datasets:` section
- `src/checks/dq_checks.py` — added `check_all_schemas()` and `run_all_checks()`
- `tests/test_run_all_checks.py` — new test file (6 tests, all passing)
- `README.md` — updated examples, structure, roadmap
- `docs/orchestrator-build-log.md` — this file

## Test Results
```
======================================================================
RUNNING ORCHESTRATOR TESTS — run_all_checks()
======================================================================

✓ Test: Multiple datasets return results — 8 checks across 2 datasets
✓ Test: Only declared checks run — skipped all but completeness
✓ Test: Empty datasets config returns empty results
✓ Test: DQ score calculated — score=100.0, grade=A
✓ Test: No scoring config → no score appended
✓ Test: All 8 results have 'passed' boolean

======================================================================
ORCHESTRATOR TESTS COMPLETE — 6 passed, 0 failed
======================================================================
```