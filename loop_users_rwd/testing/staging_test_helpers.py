"""Reusable test helpers for loop_users_rwd data staging tests.

Run on Databricks with access to `dev.loop_users_rwd`.
"""

import pandas as pd

TEST_SCHEMA = "dev.loop_users_rwd"


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
