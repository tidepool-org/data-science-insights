"""
Unit test for export_stable_autobolus_segments.py.

Tests: 14-day sliding window produces all scored windows (unfiltered,
auditable) with coverage, autobolus_pct, and days_since_first_ab.
All users appear — including low coverage and no-autobolus users.
Rows are ranked by segment_start per user.

Run on Databricks.
"""

import sys
from datetime import date

from pyspark.sql import SparkSession  # type: ignore

sys.path.insert(0, "../data_staging")

from export_stable_autobolus_segments import run  # type: ignore # noqa: E402
from staging_test_helpers import (  # noqa: E402
    TEST_SCHEMA,
    make_loop_recs,
    read_test_output,
    setup_test_table,
    teardown_test_tables,
)

spark = SparkSession.builder.getOrCreate()

# --- Table names ---
RECS_TABLE = f"{TEST_SCHEMA}._test_sas_recs"
DATES_TABLE = f"{TEST_SCHEMA}._test_sas_dates"
GENDER_TABLE = f"{TEST_SCHEMA}._test_sas_gender"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_sas_output"

ALL_TABLES = [RECS_TABLE, DATES_TABLE, GENDER_TABLE, OUTPUT_TABLE]

# --- Test data ---
# user_qualifies: 45 days all-autobolus, 288 rows/day. High coverage, AB%=100%.
# user_low_coverage: 45 days all-autobolus, 100 rows/day. Coverage ~35% < 70%.
# user_no_ab: 45 days temp basal, 288 rows/day. No first_ab_day, AB%=0%.
recs_rows = (
    make_loop_recs("user_qualifies", date(2025, 1, 1), 45, 288, 1)
    + make_loop_recs("user_low_coverage", date(2025, 1, 1), 45, 100, 1)
    + make_loop_recs("user_no_ab", date(2025, 1, 1), 45, 288, 0)
)

dates_rows = [
    {"userid": "user_qualifies", "dob": date(1990, 6, 15), "diagnosis_date": date(2005, 3, 1)},
    {"userid": "user_low_coverage", "dob": date(1985, 1, 1), "diagnosis_date": date(2000, 1, 1)},
    {"userid": "user_no_ab", "dob": date(1992, 1, 1), "diagnosis_date": date(2010, 1, 1)},
]

gender_rows = [
    {"userid": "user_qualifies", "gender": "F"},
    {"userid": "user_low_coverage", "gender": "M"},
    {"userid": "user_no_ab", "gender": "F"},
]

# --- Run test ---
try:
    setup_test_table(spark, RECS_TABLE, recs_rows)
    setup_test_table(spark, DATES_TABLE, dates_rows)
    setup_test_table(spark, GENDER_TABLE, gender_rows)

    run(
        spark,
        output_table=OUTPUT_TABLE,
        loop_recommendations_table=RECS_TABLE,
        user_dates_table=DATES_TABLE,
        user_gender_table=GENDER_TABLE,
    )

    result = read_test_output(spark, OUTPUT_TABLE)

    # 1. All three users appear (no filtering)
    user_ids = set(result["_userId"].tolist())
    assert user_ids == {"user_qualifies", "user_low_coverage", "user_no_ab"}, (
        f"expected all 3 users, got {user_ids}"
    )
    print(f"PASS: all 3 users in results ({len(result)} total windows)")

    # 2. rn column present and starts at 1 per user
    assert "rn" in result.columns, "missing rn column"
    for uid in user_ids:
        user_rn = result[result["_userId"] == uid]["rn"]
        assert int(user_rn.min()) == 1, f"rn should start at 1 for {uid}, got {user_rn.min()}"
    print("PASS: rn column present, starts at 1 per user")

    # 3. user_qualifies: coverage range spans low (early) to high (later)
    uq = result[result["_userId"] == "user_qualifies"]
    uq_cov = uq["coverage"].astype(float)
    assert uq_cov.min() < 0.70, f"min coverage should be < 0.70, got {uq_cov.min()}"
    assert uq_cov.max() >= 0.70, f"max coverage should be >= 0.70, got {uq_cov.max()}"
    uq_full = uq[uq_cov >= 0.70]
    uq_high = uq_full.iloc[0]
    assert float(uq_high["autobolus_pct"]) == 1.0, (
        f"autobolus_pct {uq_high['autobolus_pct']} != 1.0"
    )
    print(f"PASS: user_qualifies — coverage [{uq_cov.min():.3f}, {uq_cov.max():.3f}], {len(uq_full)} windows >= 0.70")

    # 4. user_low_coverage: low coverage visible in output
    ulc = result[result["_userId"] == "user_low_coverage"]
    assert all(ulc["coverage"].astype(float) < 0.70), (
        f"user_low_coverage should have coverage < 0.70, got {ulc['coverage'].tolist()}"
    )
    print(f"PASS: user_low_coverage — all {len(ulc)} windows have coverage < 0.70")

    # 5. user_no_ab: no first_ab_day, autobolus_pct = 0, days_since_first_ab is NULL
    una = result[result["_userId"] == "user_no_ab"]
    assert all(una["first_ab_day"].isna()), "user_no_ab should have NULL first_ab_day"
    assert all(una["autobolus_pct"].astype(float) == 0.0), (
        f"user_no_ab should have autobolus_pct = 0, got {una['autobolus_pct'].tolist()}"
    )
    assert all(una["days_since_first_ab"].isna()), "user_no_ab should have NULL days_since_first_ab"
    print(f"PASS: user_no_ab — {len(una)} windows, NULL first_ab_day, autobolus_pct=0")

    # 6. days_since_first_ab increases over windows for user_qualifies
    dsfa = uq.sort_values("rn")["days_since_first_ab"].astype(int)
    assert dsfa.iloc[-1] > dsfa.iloc[0], "days_since_first_ab should increase across windows"
    print(f"PASS: days_since_first_ab ranges from {dsfa.iloc[0]} to {dsfa.iloc[-1]}")

    # 7. Demographics joined correctly
    assert uq_high["gender"] == "F", f"expected gender F, got {uq_high['gender']}"
    assert uq_high["dob"] is not None, "dob should be populated"
    assert float(uq_high["age_years"]) > 30, f"age_years {uq_high['age_years']} should be > 30"
    print("PASS: demographics joined correctly")

    print("\nAll tests passed.")

finally:
    teardown_test_tables(spark, *ALL_TABLES)
