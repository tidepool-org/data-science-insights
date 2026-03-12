"""
Create test device data for optimized_users.py end-to-end SQL testing.

Writes to dev.fda_510k_rwd.test_optimized_users_data with the same column
format as bddp_sample_all. One user, 35 days:

  - Days  0–14:  autobolus + silent  (no bolus, no carbs)   15 days
  - Days 15–19:  autobolus + carbs only (no bolus)           5 days
  - Days 20–24:  autobolus + bolus only (no carbs)           5 days
  - Days 25–29:  autobolus + bolus + carbs                   5 days
  - Days 30–34:  temp_basal + no bolus (padding)             5 days

The user has 20 no-bolus days (15 silent + 5 carbs-only), so qualifies
(>=10 no-bolus days). They have autobolus+silent days, so pass the Python
`users_with_silent` filter.

Each day has 200 CBG readings at 5-min intervals with fixed mg/dL values
per glycemic bucket, 10 Loop recommendation rows, and optionally 1 manual
bolus row and/or 1 carb announcement row.
"""

import json
from datetime import datetime, timedelta

import pandas as pd

# pyright: reportMissingImports=false
spark = spark  # type: ignore[name-defined]  # noqa: F841

# =============================================================================
# Config
# =============================================================================

USER_ID = "test_user_optimized"
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
    "ab_silent": {
        "counts": {"below_54": 160, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": True,
        "is_bolus_day": False,
        "has_carbs": False,
    },
    "ab_carbs_only": {
        "counts": {"below_54": 10, "54_70": 160, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": True,
        "is_bolus_day": False,
        "has_carbs": True,
    },
    "ab_bolus_only": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 160, "180_250": 10, "above_250": 10},
        "is_autobolus": True,
        "is_bolus_day": True,
        "has_carbs": False,
    },
    "ab_bolus_carbs": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 10, "180_250": 160, "above_250": 10},
        "is_autobolus": True,
        "is_bolus_day": True,
        "has_carbs": True,
    },
    "tb_no_bolus": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 160},
        "is_autobolus": False,
        "is_bolus_day": False,
        "has_carbs": False,
    },
}

# How many days per group
DAYS_PER_GROUP = {
    "ab_silent": 15,
    "ab_carbs_only": 5,
    "ab_bolus_only": 5,
    "ab_bolus_carbs": 5,
    "tb_no_bolus": 5,
}

NUTRITION_JSON = json.dumps({"carbohydrate": {"net": 34.0, "units": "grams"}})

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
                "nutrition": None,
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
            "nutrition": None,
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
        "nutrition": None,
    }


def _make_carb_row(user_id, day_start):
    """Create a single carb announcement row (type='food')."""
    ts = day_start + timedelta(hours=8)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "food",
        "subType": None,
        "reason": None,
        "origin": None,
        "value": None,
        "normal": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "nutrition": NUTRITION_JSON,
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
        if config["has_carbs"]:
            rows.append(_make_carb_row(USER_ID, day_start))
        day_offset += 1

# =============================================================================
# Write to Databricks
# =============================================================================

TABLE = "dev.fda_510k_rwd.test_optimized_users_data"

df = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable(TABLE)

print(f"Created {TABLE}: {len(df):,} rows, {day_offset} days")
print(f"  CBG rows:  {len(df[df['type'] == 'cbg']):,}")
print(f"  Loop recs: {len(df[df['reason'] == 'loop']):,}")
print(f"  Bolus rows: {len(df[df['type'] == 'bolus']):,}")
print(f"  Carb rows:  {len(df[df['type'] == 'food']):,}")
