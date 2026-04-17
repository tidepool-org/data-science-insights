"""
Reusable test helpers for FDA 510(k) data staging query tests.
Run on Databricks with access to dev.fda_510k_rwd.
"""

from datetime import timedelta

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


def make_loop_recs(user_id, start_date, n_days, is_autobolus,
                   autobolus_count=10, temp_basal_count=10, loop_version="3.2.0"):
    """Generate per-day rows matching the loop_recommendations schema.

    One row per (user, day) with the four method-specific counts. For each day,
    either the autobolus or temp_basal counts are populated (never both) based
    on `is_autobolus`. `hk_*` counts are left NULL to keep the fixture focused
    on dosingDecision-derived signals.

    Args:
        user_id: _userId value
        start_date: first day (date object)
        n_days: number of days to generate
        is_autobolus: 1 → autobolus day, 0 → temp_basal day
        autobolus_count: dd_autobolus_count when is_autobolus=1
        temp_basal_count: dd_temp_basal_count when is_autobolus=0
        loop_version: string like "3.2.0"
    """
    parts = (loop_version.split(".") + ["0", "0", "0"])[:3]
    version_int = int(parts[0]) * 1_000_000 + int(parts[1]) * 1_000 + int(parts[2])

    rows = []
    for d in range(n_days):
        day = start_date + timedelta(days=d)
        rows.append({
            "_userId": user_id,
            "day": day,
            "dd_autobolus_count": autobolus_count if is_autobolus else None,
            "hk_autobolus_count": None,
            "dd_temp_basal_count": None if is_autobolus else temp_basal_count,
            "hk_temp_basal_count": None,
            "loop_version": loop_version,
            "version_int": version_int,
        })
    return rows
