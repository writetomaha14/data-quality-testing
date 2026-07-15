# Repo Reorganization Log

**Date:** July 15, 2026
**Purpose:** Reorganize `data-quality-testing` from a flat file list into a structured portfolio layout, before continuing daily PySpark/DQ work.

---

## Before

```
data-quality-testing/
├── README.md
├── schema_validation.py
├── data_accuracy.py
├── data_completeness
└── test_connection.ipynb
```

## After

```
data-quality-testing/
├── README.md
├── notebooks/
│   ├── phase1_foundations/
│   │   └── day01_setup_test.ipynb        (renamed from test_connection.ipynb)
│   ├── phase2_dq_framework/               (empty — for Day 31-60 notebooks)
│   └── phase3_production/                 (empty — for Day 61-90 notebooks)
├── src/
│   ├── checks/
│   │   ├── completeness.py                (renamed from data_completeness)
│   │   ├── data_accuracy.py
│   │   └── schema_validation.py
│   ├── config/                             (empty — for Day 25/79 YAML configs)
│   └── utils/                              (empty — for Day 24 logging utilities)
├── tests/                                  (empty — for Day 23/50 pytest files)
└── docs/                                   (this file lives here)
```

---

## Steps taken

All commands were run from inside a Databricks notebook cell using `%sh`, from the repo root:
`/Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing`

**1. Confirmed repo location**
```
%sh pwd
```

**2. Created the new folder structure**
```
%sh
mkdir -p notebooks/phase1_foundations
mkdir -p notebooks/phase2_dq_framework
mkdir -p notebooks/phase3_production
mkdir -p src/checks
mkdir -p src/config
mkdir -p src/utils
mkdir -p tests
mkdir -p docs
```

**3. Moved existing framework files into `src/checks/`**
```
%sh
mv schema_validation.py src/checks/schema_validation.py
mv data_accuracy.py src/checks/data_accuracy.py
mv data_completeness src/checks/completeness.py
```

**4. Moved the test notebook using the Databricks UI (not `mv`)**
Used Workspace file browser → three-dot menu → Move, since the notebook was open and active at the time. Moved `test_connection.ipynb` into `notebooks/phase1_foundations/` and renamed it to `day01_setup_test.ipynb`.

**5. Verified the final structure**
```
%sh
cd /Workspace/Repos/maha.b.lakshmi@gmail.com/data-quality-testing
find . -not -path "./.git/*" -type f
```
Confirmed output matched the "After" structure above.

**6. Committed and pushed**
Commit message:
```
Reorganize repo: separate notebooks, src framework, tests, and docs
```
9 changed files (deletions of old paths + additions at new paths). Pushed successfully — confirmed live on GitHub.

---

## Notes for future days

- New daily notebooks go under `notebooks/<phase-folder>/dayNN_topic.ipynb`.
- Anything promoted from a notebook into a reusable, tested module goes under `src/checks/`, `src/config/`, or `src/utils/` as appropriate.
- `tests/` and `docs/` won't appear on GitHub until they contain at least one file — Git doesn't track empty folders. Add a `.gitkeep` placeholder if you want them visible before real content exists.
- A one-off "repo health check" notebook (originally created to verify this reorg) was intentionally kept at the repo root for future reference rather than deleted.
