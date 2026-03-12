"""
Create test device data for no_bolus_day_analysis.py end-to-end SQL testing.

Writes to dev.fda.test_no_bolus_device_data with the same column format as
bddp_sample_all. One user, 30 days:
  - Days  0–9:  temp_basal + no_bolus  (10 days)
  - Days 10–19: autobolus + no_bolus   (10 days)
  - Days 20–24: temp_basal + bolus     (5 days)
  - Days 25–29: autobolus + bolus      (5 days)

Each day has 200 CBG readings at 5-min intervals with fixed mg/dL values
per glycemic bucket, 10 Loop recommendation rows, and (for bolus days)
1 manual bolus row.
"""

import json
from datetime import datetime, timedelta

import pandas as pd

# pyright: reportMissingImports=false
spark = spark  # type: ignore[name-defined]  # noqa: F841

# =============================================================================
# Config
# =============================================================================

USER_ID = "test_user_no_bolus"
START = datetime(2024, 1, 1)
ORIGIN = json.dumps({"version": "3.2.0"})
MMOL_PER_MGDL = 1 / 18.018
INTERVAL_MIN = 5
N_CBG = 200  # readings per day (must be >= 200 for HAVING filter)
N_RECS = 10  # loop recommendation rows per day

# Fixed CBG values per glycemic bucket (mg/dL), placed far from boundaries
BUCKET_VALUES = {
    "below_54": 50.0,
    "54_70": 60.0,
    "70_180": 125.0,
    "180_250": 210.0,
    "above_250": 280.0,
}

# Per-group CBG bucket counts (must sum to N_CBG=200)
GROUP_CONFIGS = {
    "temp_basal_no_bolus": {
        "counts": {"below_54": 160, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": False,
        "is_bolus_day": False,
    },
    "autobolus_no_bolus": {
        "counts": {"below_54": 10, "54_70": 160, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": True,
        "is_bolus_day": False,
    },
    "temp_basal_bolus": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 160, "180_250": 10, "above_250": 10},
        "is_autobolus": False,
        "is_bolus_day": True,
    },
    "autobolus_bolus": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 10, "180_250": 160, "above_250": 10},
        "is_autobolus": True,
        "is_bolus_day": True,
    },
}

# How many days per group
DAYS_PER_GROUP = {
    "temp_basal_no_bolus": 10,
    "autobolus_no_bolus": 10,
    "temp_basal_bolus": 5,
    "autobolus_bolus": 5,
}

# =============================================================================
# Row generators
# =============================================================================

def _time_json(ts):
    """Format a timestamp as the JSON column bddp_sample_all expects."""
    return json.dumps({"$date": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")})


def _make_cbg_rows(user_id, day_start, bucket_counts):
    """Create CBG rows with fixed values per glycemic bucket."""
    rows = []
    i = 0
    for bucket, count in bucket_counts.items():
        mg_dl = BUCKET_VALUES[bucket]
        for _ in range(count):
            ts = day_start + timedelta(minutes=INTERVAL_MIN * i)
            rows.append({
                "_userId": user_id,
                "time": _time_json(ts),
                "type": "cbg",
                "subType": None,
                "reason": None,
                "origin": None,
                "value": mg_dl * MMOL_PER_MGDL,
                "normal": None,
                "recommendedBasal": None,
                "recommendedBolus": None,
            })
            i += 1
    return rows


def _make_loop_rec_rows(user_id, day_start, is_autobolus):
    """Create Loop recommendation rows (reason='loop')."""
    rows = []
    for j in range(N_RECS):
        ts = day_start + timedelta(hours=j)
        rows.append({
            "_userId": user_id,
            "time": _time_json(ts),
            "type": None,
            "subType": None,
            "reason": "loop",
            "origin": ORIGIN,
            "value": None,
            "normal": None,
            "recommendedBasal": None if is_autobolus else 1.0,
            "recommendedBolus": 0.5 if is_autobolus else None,
        })
    return rows


def _make_bolus_row(user_id, day_start):
    """Create a single manual bolus row (type='bolus', subType='normal')."""
    ts = day_start + timedelta(hours=12)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "bolus",
        "subType": "normal",
        "reason": None,
        "origin": None,
        "value": None,
        "normal": 5.0,
        "recommendedBasal": None,
        "recommendedBolus": None,
    }

# =============================================================================
# Build all rows
# =============================================================================

rows = []
day_offset = 0

for group_type, n_days in DAYS_PER_GROUP.items():
    config = GROUP_CONFIGS[group_type]
    for _ in range(n_days):
        day_start = START + timedelta(days=day_offset)
        rows += _make_cbg_rows(USER_ID, day_start, config["counts"])
        rows += _make_loop_rec_rows(USER_ID, day_start, config["is_autobolus"])
        if config["is_bolus_day"]:
            rows.append(_make_bolus_row(USER_ID, day_start))
        day_offset += 1

# =============================================================================
# Write to Databricks
# =============================================================================

TABLE = "dev.fda_510k_rwd.test_no_bolus_device_data"

df = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable(TABLE)

print(f"Created {TABLE}: {len(df):,} rows, {day_offset} days")
print(f"  CBG rows:  {len(df[df['type'] == 'cbg']):,}")
print(f"  Loop recs: {len(df[df['reason'] == 'loop']):,}")
print(f"  Bolus rows: {len(df[df['type'] == 'bolus']):,}")
