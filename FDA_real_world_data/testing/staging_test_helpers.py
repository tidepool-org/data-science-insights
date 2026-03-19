"""
Reusable test helpers for FDA 510(k) data staging query tests.
Run on Databricks with access to dev.fda_510k_rwd.
"""

from datetime import datetime, timedelta

import pandas as pd

TEST_SCHEMA = "dev.fda_510k_rwd"


def setup_test_table(spark, table_name, rows):
    """Write test rows to a Unity Catalog table."""
    df = pd.DataFrame(rows)
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(table_name)
    print(f"Setup: wrote {len(rows)} rows to {table_name}")


def teardown_test_tables(spark, *table_names):
    """Drop test tables."""
    for t in table_names:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print(f"Teardown: dropped {t}")


def read_test_output(spark, table_name):
    """Read a test output table into a pandas DataFrame."""
    return spark.table(table_name).toPandas()


def assert_row_count(df, expected, label=""):
    """Assert exact row count."""
    actual = len(df)
    assert actual == expected, f"{label}: expected {expected} rows, got {actual}"
    print(f"PASS: {label} — row count = {expected}")


def assert_column_values(df, column, expected_values, label=""):
    """Assert that a column contains exactly the expected values (order-insensitive, NULL-aware)."""
    actual = sorted(df[column].dropna().tolist())
    expected = sorted([v for v in expected_values if v is not None])
    assert actual == expected, f"{label}: expected {expected}, got {actual}"

    actual_nulls = int(df[column].isna().sum())
    expected_nulls = sum(1 for v in expected_values if v is None)
    assert actual_nulls == expected_nulls, (
        f"{label}: expected {expected_nulls} NULLs, got {actual_nulls}"
    )
    print(f"PASS: {label}")


def make_loop_recs(user_id, start_date, n_days, rows_per_day, is_autobolus):
    """Generate rows in the loop_recommendations table format.

    Args:
        user_id: _userId value
        start_date: first day (date object)
        n_days: number of days to generate
        rows_per_day: number of rows per day (288 = 5-min intervals)
        is_autobolus: constant value (0 or 1) for all rows
    """
    rows = []
    for d in range(n_days):
        day = start_date + timedelta(days=d)
        for i in range(rows_per_day):
            ts = datetime.combine(day, datetime.min.time()) + timedelta(minutes=5 * i)
            rows.append({
                "_userId": user_id,
                "settings_time": ts,
                "day": day,
                "is_autobolus": is_autobolus,
            })
    return rows
