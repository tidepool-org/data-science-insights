"""
Spark test for `data_staging/export_user_day_tdd.py` — delivered TDD = basal + bolus.

Covers the two delivery streams the synthetic integration fixture never exercises (it emits only
HealthKit-origin rows): the Loop-direct fallback, the HealthKit-preferred coalesce, the
commanded-vs-delivered subtlety (Loop-direct `rate` ≈ 1.7× `payload.deliveredUnits` — only the
delivered amount counts), and the nearest-minute dedup of re-ingest + dual-sync duplicates.

Runs on Databricks (or local pyspark); SKIPS cleanly elsewhere. Output is silent on success — the
assertions raise on failure (the suite reports just pass/fail).
"""

import os
import sys
from datetime import date

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:  # Databricks notebook view doesn't define __file__.
    _here = ("/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/"
             "no_meal_announcement/testing/data_staging")
sys.path.insert(0, os.path.join(_here, "..", "..", "data_staging"))
sys.path.insert(0, os.path.join(_here, ".."))

# One clean module-level skip when Spark is unavailable (no per-test NotImplementedError noise).
try:
    from pyspark.sql import SparkSession  # type: ignore
    spark = SparkSession.builder.getOrCreate()
except (ImportError, RuntimeError) as _spark_err:
    import pytest
    pytest.skip(f"Spark unavailable locally ({_spark_err}); run on Databricks.",
                allow_module_level=True)

from export_user_day_tdd import run  # type: ignore # noqa: E402

from nma_test_helpers import (  # type: ignore # noqa: E402
    make_basal_row,
    make_bolus_events,
    make_loop_direct_basal_row,
    read_test_output,
    setup_test_table,
)
from FDA_real_world_data.testing.staging_test_helpers import (  # type: ignore # noqa: E402
    TEST_SCHEMA,
    teardown_test_tables,
)

INPUT_TABLE = f"{TEST_SCHEMA}._test_input_tdd"
OUTPUT_TABLE = f"{TEST_SCHEMA}._test_output_tdd"
ALL_TABLES = [INPUT_TABLE, OUTPUT_TABLE]


# ---------------------------------------------------------------------------
# Fixture — one user per day, each isolating a TDD code path.
# ---------------------------------------------------------------------------

def _at(row, iso):
    """Copy a row to a specific time_string (+ created_timestamp) — for the dedup duplicates."""
    return {**row, "time_string": iso, "created_timestamp": iso}


ROWS = []

# Day 1 (user_hk): HealthKit basal rate 2.0 U/hr × 24h → basal 48.0, no bolus.
ROWS.append(make_basal_row("user_hk", date(2025, 2, 1), rate_u_per_hr=2.0))

# Day 2 (user_bolus): HK basal 1.0 → 24; 3 manual normal boluses (2.0 ea = 6) + 2 autoboluses
# (1.0 ea = 2) → bolus 8.0 (autoboluses ARE delivered insulin → included), tdd 32.
ROWS.append(make_basal_row("user_bolus", date(2025, 2, 2), rate_u_per_hr=1.0))
ROWS.extend(make_bolus_events("user_bolus", date(2025, 2, 2),
                              n_non_meal=3, non_meal_units=2.0, n_autobolus=2, autobolus_units=1.0))

# Day 3 (user_ld): Loop-direct ONLY — delivered 10.0, commanded rate ≈1.7×(10/24). TDD must use
# payload.deliveredUnits (10.0), NOT rate×duration (≈17.0); basal_source='loop_direct'.
ROWS.append(make_loop_direct_basal_row("user_ld", date(2025, 2, 3), delivered_units=10.0))

# Day 4 (user_both): HK basal 1.0 → 24 AND a Loop-direct basal delivering 99 on the same day.
# HealthKit is preferred (not summed) → basal 24.0, basal_source='healthkit'.
ROWS.append(make_basal_row("user_both", date(2025, 2, 4), rate_u_per_hr=1.0))
ROWS.append(make_loop_direct_basal_row("user_both", date(2025, 2, 4), delivered_units=99.0))

# Day 5 (user_dedup): HK basal 1.0 → 24; one logical 3.0 bolus written THREE times — exact re-ingest
# (same ts) + a ~15s dual-sync offset. All round to the same minute + same value → counted ONCE →
# bolus 3.0 (not 9.0), tdd 27.
ROWS.append(make_basal_row("user_dedup", date(2025, 2, 5), rate_u_per_hr=1.0))
_b = make_bolus_events("user_dedup", date(2025, 2, 5), n_non_meal=1, non_meal_units=3.0)[0]
ROWS.append(_at(_b, "2025-02-05T06:00:00Z"))   # original
ROWS.append(_at(_b, "2025-02-05T06:00:00Z"))   # exact re-ingest
ROWS.append(_at(_b, "2025-02-05T06:00:15Z"))   # dual-sync ~15s offset

# The row builders emit BDDP fields this fixture never uses (e.g. nutrition/reason are all-None here).
# setup_test_table infers the Spark schema, and an all-null column has no inferable type — so drop any
# column that is None in every row before writing. (The TDD SQL reads type/origin/payload/rate/
# duration/normal/time_string/created_timestamp, all of which keep at least one non-null value.)
_keep = {k for k in set().union(*[r.keys() for r in ROWS]) if any(r.get(k) is not None for r in ROWS)}
ROWS = [{k: v for k, v in r.items() if k in _keep} for r in ROWS]


try:
    setup_test_table(spark, INPUT_TABLE, ROWS)
    run(spark, input_table=INPUT_TABLE, output_table=OUTPUT_TABLE)
    result = read_test_output(spark, OUTPUT_TABLE)

    by_day = {
        (r["_userId"], r["local_day"]): (
            float(r["basal_units"]), float(r["bolus_units"]),
            float(r["tdd_units"]), r["basal_source"],
        )
        for _, r in result.iterrows()
    }

    # HealthKit basal = rate × 24h.
    basal, bolus, tdd, src = by_day[("user_hk", date(2025, 2, 1))]
    assert abs(basal - 48.0) < 0.01 and bolus == 0.0 and src == "healthkit", by_day[("user_hk", date(2025, 2, 1))]

    # Bolus sum includes autoboluses (delivered insulin).
    basal, bolus, tdd, src = by_day[("user_bolus", date(2025, 2, 2))]
    assert abs(bolus - 8.0) < 0.01, f"bolus_units should sum manual + auto = 8.0, got {bolus}"
    assert abs(tdd - 32.0) < 0.01, f"tdd = basal + bolus = 32.0, got {tdd}"

    # Loop-direct: delivered units, NOT commanded rate × duration.
    basal, bolus, tdd, src = by_day[("user_ld", date(2025, 2, 3))]
    assert abs(basal - 10.0) < 0.01, f"Loop-direct basal must use deliveredUnits (10.0), got {basal}"
    assert src == "loop_direct", f"basal_source should be loop_direct, got {src}"

    # Both streams on one day → HealthKit preferred, not summed.
    basal, bolus, tdd, src = by_day[("user_both", date(2025, 2, 4))]
    assert abs(basal - 24.0) < 0.01 and src == "healthkit", (
        f"both-streams day should take HK basal (24.0), got {basal} / {src}")

    # Nearest-minute dedup: 3 copies of one 3.0 bolus → counted once.
    basal, bolus, tdd, src = by_day[("user_dedup", date(2025, 2, 5))]
    assert abs(bolus - 3.0) < 0.01, f"deduped bolus should be 3.0 (not 9.0), got {bolus}"

finally:
    teardown_test_tables(spark, *ALL_TABLES)
