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

def check_referential_integrity(child_df, child_key, parent_df, parent_key):
    """
    Check whether every value in child_df's foreign key column exists
    in parent_df's corresponding key column.

    Args:
        child_df: the DataFrame with the foreign key (e.g. orders).
        child_key: the foreign key column name in child_df.
        parent_df: the DataFrame that should contain all valid keys (e.g. customers).
        parent_key: the key column name in parent_df.

    Returns:
        A dict with check name, total child rows, orphan count, and
        whether the relationship is fully intact.
    """
    total = child_df.count()
    joined = child_df.join(
        parent_df.select(parent_key).distinct(),
        child_df[child_key] == parent_df[parent_key],
        "left"
    )
    orphans = joined.filter(parent_df[parent_key].isNull()).count()
    return {
        "check": "referential_integrity",
        "child_key": child_key,
        "total_rows": total,
        "orphan_count": orphans,
        "passed": orphans == 0,
    }


def get_orphaned_rows(child_df, child_key, parent_df, parent_key):
    """
    Return the actual orphaned rows from child_df - rows whose foreign
    key doesn't exist anywhere in parent_df.

    Note: unlike check_referential_integrity(), this returns a
    DataFrame, not a dict - for investigation, not summarizing.
    """
    joined = child_df.join(
        parent_df.select(parent_key).distinct(),
        child_df[child_key] == parent_df[parent_key],
        "left"
    )
    return joined.filter(parent_df[parent_key].isNull())