"""
Create test device data for optimized_users.py end-to-end SQL testing.

Writes to dev.fda_510k_rwd.test_optimized_users_data with the same column
format as bddp_sample_all. One user, 30 days:

  - Days  0–9:   No Bolus  (no dosingDecision, no food)          10 days
  - Days 10–19:  FCL       (1 correction bolus, no food)         10 days
  - Days 20–29:  HCL       (meal bolus + food announcement)      10 days

All days use autobolus recommendations. The user has 10 no-bolus days,
so qualifies (>=10 no-bolus days).

Each day has 200 CBG readings at 5-min intervals with fixed mg/dL values
per glycemic bucket, 10 Loop recommendation rows, and optionally dosing
decision and/or food announcement rows.
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
    "no_bolus": {
        "counts": {"below_54": 160, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 10},
        "has_correction_bolus": False,
        "has_meal_bolus": False,
        "has_food": False,
    },
    "fcl": {
        "counts": {"below_54": 10, "54_70": 160, "70_180": 10, "180_250": 10, "above_250": 10},
        "has_correction_bolus": True,
        "has_meal_bolus": False,
        "has_food": False,
    },
    "hcl": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 160, "180_250": 10, "above_250": 10},
        "has_correction_bolus": False,
        "has_meal_bolus": True,
        "has_food": True,
    },
}

# How many days per group
DAYS_PER_GROUP = {
    "no_bolus": 10,
    "fcl": 10,
    "hcl": 10,
}

NUTRITION_JSON = json.dumps({"carbohydrate": {"net": 34.0, "units": "grams"}})
REQUESTED_BOLUS_JSON = json.dumps({"amount": 5.0})

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
                "recommendedBasal": None,
                "recommendedBolus": None,
                "requestedBolus": None,
                "nutrition": None,
                "food": None,
                "originalFood": None,
            })
            i += 1
    return rows


def _make_loop_rec_rows(user_id, day_start):
    """Create Loop recommendation rows (reason='loop'), autobolus type."""
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
            "recommendedBasal": None,
            "recommendedBolus": 0.5,
            "requestedBolus": None,
            "nutrition": None,
            "food": None,
            "originalFood": None,
        })
    return rows


def _make_correction_bolus_row(user_id, day_start):
    """Create a correction bolus: dosingDecision with requestedBolus but no food/originalFood."""
    ts = day_start + timedelta(hours=12)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "dosingDecision",
        "subType": None,
        "reason": None,
        "origin": None,
        "value": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "requestedBolus": REQUESTED_BOLUS_JSON,
        "nutrition": None,
        "food": None,
        "originalFood": None,
    }


def _make_meal_bolus_row(user_id, day_start):
    """Create a meal bolus: dosingDecision with requestedBolus AND food entry."""
    ts = day_start + timedelta(hours=12)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "dosingDecision",
        "subType": None,
        "reason": None,
        "origin": None,
        "value": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "requestedBolus": REQUESTED_BOLUS_JSON,
        "nutrition": None,
        "food": json.dumps({"nutrition": {"carbohydrate": {"net": 34.0}}}),
        "originalFood": None,
    }


def _make_food_row(user_id, day_start):
    """Create a food announcement row (type='food')."""
    ts = day_start + timedelta(hours=8)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "food",
        "subType": None,
        "reason": None,
        "origin": None,
        "value": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "requestedBolus": None,
        "nutrition": NUTRITION_JSON,
        "food": None,
        "originalFood": None,
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
        rows += _make_loop_rec_rows(USER_ID, day_start)
        if config["has_correction_bolus"]:
            rows.append(_make_correction_bolus_row(USER_ID, day_start))
        if config["has_meal_bolus"]:
            rows.append(_make_meal_bolus_row(USER_ID, day_start))
        if config["has_food"]:
            rows.append(_make_food_row(USER_ID, day_start))
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
print(f"  Dosing decisions: {len(df[df['type'] == 'dosingDecision']):,}")
print(f"  Food rows: {len(df[df['type'] == 'food']):,}")
