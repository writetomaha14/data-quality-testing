"""
dq_checks.py

Reusable data quality check functions for PySpark DataFrames.
Each function returns a dict describing the check's result, so results
can be collected into a DataFrame and saved as a report.
"""

from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import current_timestamp, unix_timestamp
from pyspark.sql.functions import sum as spark_sum


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

def check_all_foreign_keys(table_registry, fk_config):
    """
    Run the appropriate referential integrity check for every relationship
    declared in fk_config, using DataFrames looked up from table_registry.
    Supports both single-column and composite (multi-column) foreign keys,
    based on each entry's 'composite' flag.
    """
    results = []
    for fk in fk_config:
        child_df = table_registry[fk["child_table"]]
        parent_df = table_registry[fk["parent_table"]]

        if fk.get("composite", False):
            result = check_referential_integrity_composite(
                child_df, fk["child_key"], parent_df, fk["parent_key"]
            )
        else:
            result = check_referential_integrity(
                child_df, fk["child_key"], parent_df, fk["parent_key"]
            )

        result["child_table"] = fk["child_table"]
        result["parent_table"] = fk["parent_table"]
        results.append(result)
    return results

def check_referential_integrity_composite(child_df, child_keys, parent_df, parent_keys):
    """
    Check referential integrity across a combination of columns.

    Args:
        child_df: the DataFrame with the composite foreign key.
        child_keys: a list of column names forming the foreign key.
        parent_df: the DataFrame that should contain all valid key combinations.
        parent_keys: the matching list of column names in parent_df.

    Returns:
        A dict in the same shape as check_referential_integrity().
    """
    total = child_df.count()
    join_condition = [child_df[c] == parent_df[p] for c, p in zip(child_keys, parent_keys)]
    joined = child_df.join(parent_df.select(*parent_keys).distinct(), join_condition, "left")
    orphans = joined.filter(parent_df[parent_keys[0]].isNull()).count()
    return {
        "check": "referential_integrity_composite",
        "child_key": " + ".join(child_keys),
        "total_rows": total,
        "orphan_count": orphans,
        "passed": orphans == 0,
    }

def check_schema(df, expected_schema):
    """
    Compare a DataFrame's actual schema against an expected one.

    Args:
        df: the PySpark DataFrame to check.
        expected_schema: a list of (column_name, type_name) tuples,
            e.g. [("id", "integer"), ("name", "string")].

    Returns:
        A dict with check name, missing columns, extra columns,
        type mismatches, and whether the schema matches exactly.
    """
    actual_schema = [(f.name, f.dataType.typeName()) for f in df.schema.fields]
    actual_dict = dict(actual_schema)
    expected_dict = dict(expected_schema)

    missing_columns = [c for c, _ in expected_schema if c not in actual_dict]
    extra_columns = [c for c, _ in actual_schema if c not in expected_dict]
    type_mismatches = [
        (c, expected_dict[c], actual_dict[c])
        for c in expected_dict
        if c in actual_dict and actual_dict[c] != expected_dict[c]
    ]

    return {
        "check": "schema_validation",
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_mismatches": type_mismatches,
        "passed": not missing_columns and not extra_columns and not type_mismatches,
    }

def check_freshness(df, timestamp_column, max_age_hours=24):
    """
    Check whether a table's most recent record is recent enough.

    Args:
        df: the PySpark DataFrame to check.
        timestamp_column: the column holding when each row was created/updated.
        max_age_hours: how old the most recent record is allowed to be (default 24).

    Returns:
        A dict with check name, most recent timestamp found, age in hours,
        and whether it's within the allowed freshness window.
    """
    latest = df.select(spark_max(col(timestamp_column))).collect()[0][0]
    if latest is None:
        return {"check": "freshness", "column": timestamp_column,
                "latest_timestamp": None, "age_hours": None, "passed": False}

    # Calculate age in hours using unix timestamps
    age_seconds = df.select(
        (unix_timestamp(current_timestamp()) - unix_timestamp(col(timestamp_column))).alias("age")
    ).agg(spark_max(col("age"))).collect()[0][0]
    age_hours = age_seconds / 3600

    return {
        "check": "freshness",
        "column": timestamp_column,
        "latest_timestamp": str(latest),
        "age_hours": round(age_hours, 1),
        "passed": age_hours <= max_age_hours,
    }


def check_all_freshness(table_registry, freshness_config):
    """
    Run check_freshness() for every table declared in freshness_config.
    """
    results = []
    for entry in freshness_config:
        df = table_registry[entry["table"]]
        result = check_freshness(df, entry["timestamp_column"], entry["max_age_hours"])
        result["table"] = entry["table"]
        results.append(result)
    return results

def check_consistency(df, rule_name, expression):
    """
    Check how many rows violate a given consistency rule.

    Args:
        df: the PySpark DataFrame to check.
        rule_name: a human-readable name for the rule (for reporting).
        expression: a SQL boolean expression string that should be TRUE
            for every valid row, e.g. "ship_date >= order_date".

    Returns:
        A dict with check name, rule name, violation count, and pass/fail.
        If the SQL expression is invalid, returns an error dict with passed=False.

    Examples:
        >>> check_consistency(orders_df, "valid_dates", "ship_date >= order_date")
        >>> check_consistency(orders_df, "positive_amount", "amount > 0")
        >>> check_consistency(orders_df, "valid_status", "status IN ('active', 'pending', 'closed')")
    """
    try:
        total = df.count()
        violations = df.filter(f"NOT ({expression})").count()
        return {
            "check": "consistency",
            "rule_name": rule_name,
            "expression": expression,
            "total_rows": total,
            "violation_count": violations,
            "passed": violations == 0,
        }
    except Exception as e:
        return {
            "check": "consistency",
            "rule_name": rule_name,
            "expression": expression,
            "error": str(e),
            "passed": False,
        }


def check_all_consistency(df, rules_config):
    """
    Run check_consistency() for every rule declared in rules_config.

    Args:
        df: the PySpark DataFrame to check.
        rules_config: a list of rule dicts, each with 'name' and 'expression' keys,
            e.g. [{"name": "valid_dates", "expression": "end_date >= start_date"}].

    Returns:
        A list of dicts, one per rule, in the same shape as check_consistency().

    Example:
        >>> rules = [
        ...     {"name": "valid_dates", "expression": "end_date >= start_date"},
        ...     {"name": "positive_amount", "expression": "amount > 0"},
        ... ]
        >>> check_all_consistency(orders_df, rules)
    """
    return [check_consistency(df, r["name"], r["expression"]) for r in rules_config]


def get_violating_rows(df, expression):
    """
    Return the actual rows that violate a consistency rule.

    Args:
        df: the PySpark DataFrame to check.
        expression: a SQL boolean expression string that should be TRUE
            for valid rows (same format as check_consistency).

    Returns:
        A DataFrame containing only the rows that violate the rule.
        Note: unlike check_consistency(), this returns a DataFrame, not
        a dict - this function is for investigation, not summarizing.

    Example:
        >>> violating = get_violating_rows(orders_df, "ship_date >= order_date")
        >>> violating.show()
    """
    return df.filter(f"NOT ({expression})")


# ==============================================================================
# Reconciliation Checks
# ==============================================================================

def check_record_count(source_df, target_df):
    """
    Compare row counts between source and target DataFrames.

    Args:
        source_df: the source DataFrame (e.g., upstream table).
        target_df: the target DataFrame (e.g., downstream copy).

    Returns:
        A dict with check name, source count, target count, count
        difference, and whether the counts match.
    """
    source_count = source_df.count()
    target_count = target_df.count()
    return {
        "check": "record_count_reconciliation",
        "source_count": source_count,
        "target_count": target_count,
        "count_diff": source_count - target_count,
        "passed": source_count == target_count,
    }


def check_sum_reconciliation(source_df, target_df, column_name, tolerance=0.01):
    """
    Compare the sum of a column between source and target DataFrames.

    Args:
        source_df: the source DataFrame.
        target_df: the target DataFrame.
        column_name: the numeric column to compare sums for.
        tolerance: the maximum allowed difference to still pass (default 0.01).

    Returns:
        A dict with check name, column, source sum, target sum, difference,
        and whether the sums match within tolerance.
    """
    source_sum = source_df.agg(spark_sum(col(column_name))).collect()[0][0] or 0
    target_sum = target_df.agg(spark_sum(col(column_name))).collect()[0][0] or 0
    diff = round(source_sum - target_sum, 2)
    return {
        "check": "sum_reconciliation",
        "column": column_name,
        "source_sum": source_sum,
        "target_sum": target_sum,
        "diff": diff,
        "passed": abs(diff) <= tolerance,
    }


def check_row_level_reconciliation(source_df, target_df, key_columns):
    """
    Compare rows between source and target DataFrames using key columns.

    Detects three types of differences:
    - Missing rows: present in source but absent from target.
    - Extra rows: present in target but absent from source.
    - Mismatched rows: key exists in both but non-key column values differ.

    Args:
        source_df: the source DataFrame.
        target_df: the target DataFrame.
        key_columns: a column name (str) or list of column names that
            uniquely identify each row for comparison.

    Returns:
        A dict with check name, key columns, source/target counts,
        missing/extra/mismatch counts, and whether all rows match.
    """
    if isinstance(key_columns, str):
        key_columns = [key_columns]

    source_count = source_df.count()
    target_count = target_df.count()

    # Missing: rows in source whose key doesn't exist in target
    missing = source_df.join(
        target_df.select(*key_columns).distinct(), key_columns, "left_anti"
    )
    missing_count = missing.count()

    # Extra: rows in target whose key doesn't exist in source
    extra = target_df.join(
        source_df.select(*key_columns).distinct(), key_columns, "left_anti"
    )
    extra_count = extra.count()

    # Mismatched: rows where key matches but full row differs
    matched_source = source_df.join(
        target_df.select(*key_columns).distinct(), key_columns, "inner"
    )
    matched_target = target_df.join(
        source_df.select(*key_columns).distinct(), key_columns, "inner"
    )
    mismatch_count = matched_source.exceptAll(matched_target).count()

    return {
        "check": "row_level_reconciliation",
        "key_columns": key_columns,
        "source_count": source_count,
        "target_count": target_count,
        "missing_count": missing_count,
        "extra_count": extra_count,
        "mismatch_count": mismatch_count,
        "passed": missing_count == 0 and extra_count == 0 and mismatch_count == 0,
    }


def get_mismatched_rows(source_df, target_df, key_columns):
    """
    Return the actual rows from source that don't match the target.

    Includes rows that are missing from target (key not found) and rows
    where the key exists in both but column values differ.

    Args:
        source_df: the source DataFrame.
        target_df: the target DataFrame.
        key_columns: a column name (str) or list of column names that
            uniquely identify each row for comparison.

    Returns:
        A DataFrame containing only the mismatched rows from source.
        Note: unlike check_row_level_reconciliation(), this returns a
        DataFrame, not a dict - for investigation, not summarizing.
    """
    if isinstance(key_columns, str):
        key_columns = [key_columns]

    # Missing: rows in source but not in target
    missing = source_df.join(
        target_df.select(*key_columns).distinct(), key_columns, "left_anti"
    )

    # Mismatched: rows where key matches but values differ
    matched_source = source_df.join(
        target_df.select(*key_columns).distinct(), key_columns, "inner"
    )
    matched_target = target_df.join(
        source_df.select(*key_columns).distinct(), key_columns, "inner"
    )
    mismatched = matched_source.exceptAll(matched_target)

    return missing.unionAll(mismatched)


def check_all_reconciliation(source_df, target_df, reconciliation_config):
    """
    Run all reconciliation checks declared in reconciliation_config.

    Args:
        source_df: the source DataFrame.
        target_df: the target DataFrame.
        reconciliation_config: a list of check dicts from YAML config,
            each with a 'check' key specifying the check type and any
            required parameters.
            e.g. [
                {"check": "record_count"},
                {"check": "sum", "column": "total"},
                {"check": "row_level", "key_columns": ["order_id"]}
            ]

    Returns:
        A list of dicts, one per check, in the same shape as the
        individual check functions.
    """
    results = []
    for entry in reconciliation_config:
        check_type = entry["check"]
        if check_type == "record_count":
            result = check_record_count(source_df, target_df)
        elif check_type == "sum":
            result = check_sum_reconciliation(source_df, target_df, entry["column"])
        elif check_type == "row_level":
            result = check_row_level_reconciliation(source_df, target_df, entry["key_columns"])
        else:
            result = {"check": check_type, "error": f"Unknown check type: {check_type}", "passed": False}
        results.append(result)
    return results


# ==============================================================================
# Anomaly Detection Checks
# ==============================================================================

def check_z_score_anomaly(df, column_name, threshold=3.0):
    """
    Detect anomalous values in a numeric column using the Z-score method.

    A Z-score measures how many standard deviations a value is from the
    mean.  Values with an absolute Z-score above the threshold are flagged
    as anomalies.

    Args:
        df: the PySpark DataFrame to check.
        column_name: the numeric column to analyze.
        threshold: the maximum |Z-score| allowed before a value is flagged
            as anomalous (default 3.0 — three standard deviations).

    Returns:
        A dict with check name, column, mean, stddev, threshold,
        anomaly count, anomaly percentage, and whether no anomalies
        were found (passed = True when anomaly_count == 0).
    """
    from pyspark.sql.functions import mean as spark_mean, stddev as spark_stddev, abs as spark_abs

    total = df.count()
    stats = df.agg(
        spark_mean(col(column_name)).alias("mean"),
        spark_stddev(col(column_name)).alias("stddev"),
    ).collect()[0]

    mean_val = stats["mean"]
    stddev_val = stats["stddev"]

    # If stddev is 0 or None, every value is identical — no anomalies
    if mean_val is None or stddev_val is None or stddev_val == 0:
        return {
            "check": "z_score_anomaly",
            "column": column_name,
            "mean": mean_val,
            "stddev": stddev_val,
            "threshold": threshold,
            "total_rows": total,
            "anomaly_count": 0,
            "anomaly_pct": 0.0,
            "passed": True,
        }

    anomalies = df.filter(
        spark_abs((col(column_name) - mean_val) / stddev_val) > threshold
    ).count()
    pct = round(anomalies / total * 100, 1) if total > 0 else 0.0

    return {
        "check": "z_score_anomaly",
        "column": column_name,
        "mean": round(mean_val, 2),
        "stddev": round(stddev_val, 2),
        "threshold": threshold,
        "total_rows": total,
        "anomaly_count": anomalies,
        "anomaly_pct": pct,
        "passed": anomalies == 0,
    }


def check_iqr_anomaly(df, column_name, multiplier=1.5):
    """
    Detect anomalous values in a numeric column using the IQR method.

    The interquartile range (IQR) is the range between the 25th and 75th
    percentiles.  Values below Q1 - multiplier * IQR or above
    Q3 + multiplier * IQR are flagged as anomalies.

    Args:
        df: the PySpark DataFrame to check.
        column_name: the numeric column to analyze.
        multiplier: the IQR multiplier controlling how far beyond the
            quartiles a value must be to be flagged (default 1.5).

    Returns:
        A dict with check name, column, Q1, Q3, IQR, lower/upper bounds,
        anomaly count, anomaly percentage, and pass/fail.
    """
    quantiles = df.stat.approxQuantile(column_name, [0.25, 0.75], 0.01)
    q1 = quantiles[0]
    q3 = quantiles[1]
    iqr = q3 - q1
    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    total = df.count()
    anomalies = df.filter(
        (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
    ).count()
    pct = round(anomalies / total * 100, 1) if total > 0 else 0.0

    return {
        "check": "iqr_anomaly",
        "column": column_name,
        "q1": round(q1, 2),
        "q3": round(q3, 2),
        "iqr": round(iqr, 2),
        "lower_bound": round(lower_bound, 2),
        "upper_bound": round(upper_bound, 2),
        "total_rows": total,
        "anomaly_count": anomalies,
        "anomaly_pct": pct,
        "passed": anomalies == 0,
    }


def get_anomaly_rows(df, column_name, method="z_score", threshold=3.0, multiplier=1.5):
    """
    Return the actual rows flagged as anomalous by the chosen method.

    Args:
        df: the PySpark DataFrame to check.
        column_name: the numeric column to analyze.
        method: the anomaly detection method — "z_score" or "iqr".
        threshold: the Z-score threshold (used only when method="z_score").
        multiplier: the IQR multiplier (used only when method="iqr").

    Returns:
        A DataFrame containing only the anomalous rows.
        Note: unlike the check_* functions, this returns a DataFrame,
        not a dict — for investigation, not summarizing.
    """
    from pyspark.sql.functions import mean as spark_mean, stddev as spark_stddev, abs as spark_abs

    if method == "z_score":
        stats = df.agg(
            spark_mean(col(column_name)).alias("mean"),
            spark_stddev(col(column_name)).alias("stddev"),
        ).collect()[0]
        mean_val = stats["mean"]
        stddev_val = stats["stddev"]

        if mean_val is None or stddev_val is None or stddev_val == 0:
            return df.filter(col(column_name).isNull())  # no anomalies possible

        return df.filter(
            spark_abs((col(column_name) - mean_val) / stddev_val) > threshold
        )

    elif method == "iqr":
        quantiles = df.stat.approxQuantile(column_name, [0.25, 0.75], 0.01)
        q1 = quantiles[0]
        q3 = quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        return df.filter(
            (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
        )

    else:
        raise ValueError(f"Unknown anomaly method: {method}. Use 'z_score' or 'iqr'.")


def check_all_anomalies(df, anomaly_config):
    """
    Run anomaly detection for every column/method declared in anomaly_config.

    Args:
        df: the PySpark DataFrame to check.
        anomaly_config: a list of check dicts from YAML config,
            each with a 'method' key ('z_score' or 'iqr'), a 'column'
            key, and optional 'threshold' or 'multiplier' keys.
            e.g. [
                {"method": "z_score", "column": "sales_amount", "threshold": 3.0},
                {"method": "iqr", "column": "units_sold", "multiplier": 1.5}
            ]

    Returns:
        A list of dicts, one per check, in the same shape as the
        individual check functions.
    """
    results = []
    for entry in anomaly_config:
        method = entry["method"]
        column = entry["column"]

        if method == "z_score":
            threshold = entry.get("threshold", 3.0)
            result = check_z_score_anomaly(df, column, threshold)
        elif method == "iqr":
            multiplier = entry.get("multiplier", 1.5)
            result = check_iqr_anomaly(df, column, multiplier)
        else:
            result = {"check": method, "column": column,
                      "error": f"Unknown method: {method}", "passed": False}

        results.append(result)
    return results