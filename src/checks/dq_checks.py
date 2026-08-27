"""
dq_checks.py

Reusable data quality check functions for PySpark DataFrames.
Each function returns a dict describing the check's result, so results
can be collected into a DataFrame and saved as a report.
"""

from pyspark.sql.functions import col


def check_completeness(df, column_name, max_null_pct=0):
    """
    Check what percentage of a column's values are null.

    Args:
        df: the PySpark DataFrame to check.
        column_name: the column to check for nulls.
        max_null_pct: the highest null percentage still considered a pass (default 0).

    Returns:
        A dict with check name, column, total rows, null count,
        null percentage, and whether it passed the threshold.
    """
    total = df.count()
    nulls = df.filter(col(column_name).isNull()).count()
    pct = round(nulls / total * 100, 1) if total > 0 else 0.0
    return {
        "check": "completeness",
        "column": column_name,
        "total_rows": total,
        "null_count": nulls,
        "null_pct": pct,
        "passed": pct <= max_null_pct,
    }


def check_completeness_all(df, max_null_pct=0):
    """
    Run check_completeness() across every column in a DataFrame.

    Args:
        df: the PySpark DataFrame to check.
        max_null_pct: the threshold applied to every column.

    Returns:
        A list of dicts, one per column, in the same shape as check_completeness().
    """
    return [check_completeness(df, c, max_null_pct) for c in df.columns]