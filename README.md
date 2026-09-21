# Data Quality Testing Framework
A configuration-driven Data Quality Engineering framework built in PySpark on Databricks — designed, tested, and documented from the ground up as a hands-on deep-dive into modern data quality engineering.
> Built by a QA & Data Testing professional (16+ years, including 6 years leading a QA team across UI, API, and automation testing) transitioning into hands-on data engineering. This repo is a real, ongoing record of that build — including the bugs, the wrong turns, and the fixes.
---
What This Framework Does
A reusable, config-driven rule engine that validates data across the dimensions that actually matter in production:
| Dimension | Checks |
| --- | --- |
| Completeness | Null detection and percentage, per column |
| Uniqueness | Duplicate detection, evidence rows, composite-key uniqueness |
| Referential Integrity | Orphan detection, composite keys, self-referencing keys, FK registry |
| Schema Validation | Missing/extra columns, type mismatches, config-declared expected schemas |
| Freshness | Timestamp-based staleness detection, per-table thresholds |
| Consistency | Cross-column business logic via SQL expressions |
| Business Rules | Organization-specific policy rules, with ownership tracking |
Every check is:
Configurable — thresholds, valid-value lists, and rules live in `config/config.yaml`, not hardcoded
Tested — every function has real pytest coverage, including edge cases (empty DataFrames, null handling, floating-point tolerance)
Documented — every function has a docstring explaining what it does, its arguments, and its return shape
Reusable — designed to be imported and called from any project, not copy-pasted per use
---
Repo Structure
```
data-quality-testing/
├── README.md
├── config/
│   └── config.yaml                    # thresholds, valid values, schemas, rules
├── src/
│   └── checks/
│       └── dq_checks.py               # all reusable check functions, one growing file
├── tests/
│   ├── test_completeness.py
│   ├── test_uniqueness.py
│   ├── test_referential_integrity.py
│   ├── test_foreign_keys.py
│   ├── test_schema_validation.py
│   ├── test_freshness.py
│   ├── test_consistency.py
│   ├── test_business_rules.py
│   └── test_data/                     # CSV fixtures — no inline test data
├── notebooks/
│   ├── phase1_foundations/            # Days 1-30: PySpark fundamentals
│   └── phase2_dq_framework/           # Days 31+: one demo notebook per check module
└── docs/                              # build logs, environment discoveries, design notes
```
---
Example Usage
```python
# In a Databricks notebook, attach the module via %run (use your actual workspace path):
# %run /Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing/src/checks/dq_checks
import yaml

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

# Completeness
result = check_completeness(employees_df, "salary", max_null_pct=config["thresholds"]["null_warning_pct"])

# Referential integrity, across every declared relationship
results = check_all_foreign_keys(table_registry, config["foreign_keys"])

# Schema validation against a declared contract
expected = [tuple(pair) for pair in config["expected_schemas"]["employees"]]
result = check_schema(employees_df, expected)
```
---
Tech Stack
PySpark on Databricks (Free Edition, serverless compute)
Unity Catalog Volumes for file storage
Delta Lake for reports and audit logs
pytest for testing
PyYAML for configuration
Git / GitHub for version control
---
Real Engineering Decisions Worth Noting
This framework wasn't built by following a tutorial — several design decisions came from hitting and solving real problems:
Databricks Free Edition serverless has no classic Jobs/Stages/Storage UI — used Query History and query plan diagrams instead
DBFS is disabled; local disk isn't shared between notebook and Spark compute on serverless — solved with Unity Catalog Volumes
`.cache()`/`.persist()` are unsupported on serverless entirely — confirmed via Databricks' own documentation; Delta writes used as the workaround
SQL's three-valued logic (`NULL` comparisons silently evaluate to neither true nor false) is explicitly tested and documented in the consistency-rules module, not left as a silent surprise
See `docs/` for detailed build logs on these.
---
Roadmap
- [ ] Reusable validator combining all checks into a single entry point (Phase 2)
- [ ] Severity levels and thresholds per check (Phase 2)
- [ ] Bronze/Silver/Gold pipeline integration (Phase 3)
- [ ] CI/CD via GitHub Actions + Databricks Jobs (Phase 3)
- [ ] Data quality dashboard (Phase 3)
Future direction: extending these validation principles toward AI/LLM output evaluation (e.g. RAG pipeline data quality, retrieval assurance) as that discipline matures — a natural next step given the framework's config-driven, rule-based design.
---
About
Built as part of a structured, self-directed 120-day study plan moving from PySpark foundations into a full data quality engineering framework. Full progress log available on request.
Connect: [LinkedIn](https://www.linkedin.com/in/mahalakshmi-b-99086a40) · [GitHub](https://github.com/writetomaha14/data-quality-testing)
