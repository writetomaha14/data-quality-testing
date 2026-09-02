"""
dq_checks.py

Reusable data quality check functions for PySpark DataFrames.
Each function returns a dict describing the check's result, so results
can be collected into a DataFrame and saved as a report.
"""

from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number



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




def check_uniqueness(df, column_name):
    """
    Check whether a column contains duplicate values.

    Args:
        df: the PySpark DataFrame to check.
        column_name: the column expected to be unique (e.g. an ID).

    Returns:
        A dict with check name, column, total rows, distinct count,
        duplicate count, and whether the column is fully unique.
    """
    total = df.count()
    distinct = df.select(column_name).distinct().count()
    duplicate_count = total - distinct
    return {
        "check": "uniqueness",
        "column": column_name,
        "total_rows": total,
        "distinct_count": distinct,
        "duplicate_count": duplicate_count,
        "passed": duplicate_count == 0,
    }


def get_duplicate_rows(df, column_name):
    """
    Return the actual duplicate rows for a column, keeping the first
    occurrence out of the result (only true extras are returned).

    Args:
        df: the PySpark DataFrame to check.
        column_name: the column expected to be unique.

    Returns:
        A DataFrame containing only the duplicate (non-first) rows.
        Note: unlike check_uniqueness(), this returns a DataFrame, not
        a dict - this function is for investigation, not summarizing.
    """
    window_spec = Window.partitionBy(column_name).orderBy(column_name)
    with_row_num = df.withColumn("_row_num", row_number().over(window_spec))
    return with_row_num.filter(col("_row_num") > 1).drop("_row_num")


def check_uniqueness_composite(df, columns):
    """
    Check uniqueness across a combination of columns, not just one.

    Args:
        df: the PySpark DataFrame to check.
        columns: a list of column names that together should be unique.

    Returns:
        A dict in the same shape as check_uniqueness(), with 'column'
        replaced by the combined key description.
    """
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    duplicate_count = total - distinct
    return {
        "check": "uniqueness_composite",
        "column": " + ".join(columns),
        "total_rows": total,
        "distinct_count": distinct,
        "duplicate_count": duplicate_count,
        "passed": duplicate_count == 0,
    }