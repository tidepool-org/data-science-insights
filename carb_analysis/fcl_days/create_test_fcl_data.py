"""
Create test device data for fcl_day_analysis.py end-to-end SQL testing.

Writes to dev.fda_510k_rwd.test_fcl_device_data with the same column
format as bddp_sample_all. One user, 20 days:

  - Days  0–4:   TB + FCL  (temp_basal recs, 1 correction bolus)  5 days
  - Days  5–9:   AB + FCL  (autobolus recs, 1 correction bolus)   5 days
  - Days 10–14:  TB + HCL  (temp_basal, meal bolus + food)        5 days
  - Days 15–19:  AB + HCL  (autobolus, meal bolus + food)         5 days

The user has 10 FCL days, so qualifies (>=10 no-bolus/FCL days).

Each day has 200 CBG readings at 5-min intervals with fixed mg/dL values
per glycemic bucket, 10 Loop recommendation rows, and dosing decision
and/or food announcement rows as appropriate.
"""

import json
from datetime import datetime, timedelta

import pandas as pd

# pyright: reportMissingImports=false
spark = spark  # type: ignore[name-defined]  # noqa: F841

# =============================================================================
# Config
# =============================================================================

USER_ID = "test_user_fcl"
START = datetime(2024, 1, 1)
ORIGIN = json.dumps({"version": "3.2.0"})
MMOL_PER_MGDL = 1 / 18.018
INTERVAL_MIN = 5
N_CBG = 200
N_RECS = 10

BUCKET_VALUES = {
    "below_54": 50.0,
    "54_70": 60.0,
    "70_180": 125.0,
    "180_250": 210.0,
    "above_250": 280.0,
}

GROUP_CONFIGS = {
    "tb_fcl": {
        "counts": {"below_54": 160, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": False,
        "has_correction_bolus": True,
        "has_meal_bolus": False,
        "has_food": False,
    },
    "ab_fcl": {
        "counts": {"below_54": 10, "54_70": 160, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": True,
        "has_correction_bolus": True,
        "has_meal_bolus": False,
        "has_food": False,
    },
    "tb_hcl": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 160, "180_250": 10, "above_250": 10},
        "is_autobolus": False,
        "has_correction_bolus": False,
        "has_meal_bolus": True,
        "has_food": True,
    },
    "ab_hcl": {
        "counts": {"below_54": 10, "54_70": 10, "70_180": 10, "180_250": 160, "above_250": 10},
        "is_autobolus": True,
        "has_correction_bolus": False,
        "has_meal_bolus": True,
        "has_food": True,
    },
}

DAYS_PER_GROUP = {
    "tb_fcl": 5,
    "ab_fcl": 5,
    "tb_hcl": 5,
    "ab_hcl": 5,
}

NUTRITION_JSON = json.dumps({"carbohydrate": {"net": 34.0, "units": "grams"}})
REQUESTED_BOLUS_JSON = json.dumps({"amount": 5.0})

# =============================================================================
# Row generators
# =============================================================================

def _time_json(ts):
    return json.dumps({"$date": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")})


def _make_cbg_rows(user_id, day_start, bucket_counts):
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
                "reason": None,
                "origin": None,
                "value": mg_dl * MMOL_PER_MGDL,
                "recommendedBasal": None,
                "recommendedBolus": None,
                "requestedBolus": None,
                "nutrition": None,
                "food": None,
            })
            i += 1
    return rows


def _make_loop_rec_rows(user_id, day_start, is_autobolus):
    rows = []
    for j in range(N_RECS):
        ts = day_start + timedelta(hours=j)
        rows.append({
            "_userId": user_id,
            "time": _time_json(ts),
            "type": None,
            "reason": "loop",
            "origin": ORIGIN,
            "value": None,
            "recommendedBasal": None if is_autobolus else 1.0,
            "recommendedBolus": 0.5 if is_autobolus else None,
            "requestedBolus": None,
            "nutrition": None,
            "food": None,
        })
    return rows


def _make_correction_bolus_row(user_id, day_start):
    ts = day_start + timedelta(hours=12)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "dosingDecision",
        "reason": None,
        "origin": None,
        "value": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "requestedBolus": REQUESTED_BOLUS_JSON,
        "nutrition": None,
        "food": None,
    }


def _make_meal_bolus_row(user_id, day_start):
    ts = day_start + timedelta(hours=12)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "dosingDecision",
        "reason": None,
        "origin": None,
        "value": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "requestedBolus": REQUESTED_BOLUS_JSON,
        "nutrition": None,
        "food": json.dumps({"nutrition": {"carbohydrate": {"net": 34.0}}}),
    }


def _make_food_row(user_id, day_start):
    ts = day_start + timedelta(hours=8)
    return {
        "_userId": user_id,
        "time": _time_json(ts),
        "type": "food",
        "reason": None,
        "origin": None,
        "value": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "requestedBolus": None,
        "nutrition": NUTRITION_JSON,
        "food": None,
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

TABLE = "dev.fda_510k_rwd.test_fcl_device_data"

df = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable(TABLE)

print(f"Created {TABLE}: {len(df):,} rows, {day_offset} days")
print(f"  CBG rows:  {len(df[df['type'] == 'cbg']):,}")
print(f"  Loop recs: {len(df[df['reason'] == 'loop']):,}")
print(f"  Dosing decisions: {len(df[df['type'] == 'dosingDecision']):,}")
print(f"  Food rows: {len(df[df['type'] == 'food']):,}")
