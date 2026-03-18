"""
Create test device data for the export_user_day_closed_loop.sql pipeline.

Writes raw device data to dev.closed_loop_rwd.test_closed_loop_device_data
with the same column format as bddp_sample_100. One user, 40 days across
8 groups that exercise all three analysis conditions:

  Group                   Days   No Bolus?  No Meal No Bolus?  FCL?
  TB No Bolus No Food      0-4   yes        yes                yes
  AB No Bolus No Food      5-9   yes        yes                yes
  TB No Bolus + Food      10-14  yes        no (has food)      no
  AB No Bolus + Food      15-19  yes        no (has food)      no
  TB FCL (1 correction)   20-24  no         no                 yes
  AB FCL (1 correction)   25-29  no         no                 yes
  TB HCL (meal + food)    30-34  HCL        HCL                HCL
  AB HCL (meal + food)    35-39  HCL        HCL                HCL

Qualifying counts:
  No Bolus (>=10):          groups 1-4 = 20 days
  No Meal No Bolus (>=10):  groups 1-2 = 10 days
  FCL (>=10):               groups 1-2 + 5-6 = 20 days
"""

import json
from datetime import datetime, timedelta

import pandas as pd

# pyright: reportMissingImports=false
spark = spark  # type: ignore[name-defined]  # noqa: F841

# =============================================================================
# Config
# =============================================================================

USER_ID = "test_user_closed_loop"
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

NUTRITION_JSON = json.dumps({"carbohydrate": {"net": 34.0, "units": "grams"}})
REQUESTED_BOLUS_JSON = json.dumps({"amount": 5.0})

GROUPS = [
    {
        "name": "tb_no_bolus_no_food",
        "days": 5,
        "counts": {"below_54": 160, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": False,
        "has_correction_bolus": False,
        "has_meal_bolus": False,
        "has_food": False,
    },
    {
        "name": "ab_no_bolus_no_food",
        "days": 5,
        "counts": {"below_54": 10, "54_70": 160, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": True,
        "has_correction_bolus": False,
        "has_meal_bolus": False,
        "has_food": False,
    },
    {
        "name": "tb_no_bolus_with_food",
        "days": 5,
        "counts": {"below_54": 10, "54_70": 10, "70_180": 160, "180_250": 10, "above_250": 10},
        "is_autobolus": False,
        "has_correction_bolus": False,
        "has_meal_bolus": False,
        "has_food": True,
    },
    {
        "name": "ab_no_bolus_with_food",
        "days": 5,
        "counts": {"below_54": 10, "54_70": 10, "70_180": 10, "180_250": 160, "above_250": 10},
        "is_autobolus": True,
        "has_correction_bolus": False,
        "has_meal_bolus": False,
        "has_food": True,
    },
    {
        "name": "tb_fcl_correction",
        "days": 5,
        "counts": {"below_54": 10, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 160},
        "is_autobolus": False,
        "has_correction_bolus": True,
        "has_meal_bolus": False,
        "has_food": False,
    },
    {
        "name": "ab_fcl_correction",
        "days": 5,
        "counts": {"below_54": 160, "54_70": 10, "70_180": 10, "180_250": 10, "above_250": 10},
        "is_autobolus": True,
        "has_correction_bolus": True,
        "has_meal_bolus": False,
        "has_food": False,
    },
    {
        "name": "tb_hcl",
        "days": 5,
        "counts": {"below_54": 10, "54_70": 10, "70_180": 160, "180_250": 10, "above_250": 10},
        "is_autobolus": False,
        "has_correction_bolus": False,
        "has_meal_bolus": True,
        "has_food": True,
    },
    {
        "name": "ab_hcl",
        "days": 5,
        "counts": {"below_54": 10, "54_70": 10, "70_180": 10, "180_250": 160, "above_250": 10},
        "is_autobolus": True,
        "has_correction_bolus": False,
        "has_meal_bolus": True,
        "has_food": True,
    },
]

# =============================================================================
# Row generators
# =============================================================================

def _time_struct(ts):
    return {"$date": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")}


def _make_cbg_rows(user_id, day_start, bucket_counts):
    rows = []
    i = 0
    for bucket, count in bucket_counts.items():
        mg_dl = BUCKET_VALUES[bucket]
        for _ in range(count):
            ts = day_start + timedelta(minutes=INTERVAL_MIN * i)
            rows.append({
                "_userId": user_id,
                "time": _time_struct(ts),
                "type": "cbg",
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


def _make_loop_rec_rows(user_id, day_start, is_autobolus):
    rows = []
    for j in range(N_RECS):
        ts = day_start + timedelta(hours=j)
        rows.append({
            "_userId": user_id,
            "time": _time_struct(ts),
            "type": None,
            "reason": "loop",
            "origin": ORIGIN,
            "value": None,
            "recommendedBasal": None if is_autobolus else 1.0,
            "recommendedBolus": 0.5 if is_autobolus else None,
            "requestedBolus": None,
            "nutrition": None,
            "food": None,
            "originalFood": None,
        })
    return rows


def _make_correction_bolus_row(user_id, day_start):
    ts = day_start + timedelta(hours=12)
    return {
        "_userId": user_id,
        "time": _time_struct(ts),
        "type": "dosingDecision",
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
    ts = day_start + timedelta(hours=12)
    return {
        "_userId": user_id,
        "time": _time_struct(ts),
        "type": "dosingDecision",
        "reason": None,
        "origin": None,
        "value": None,
        "recommendedBasal": None,
        "recommendedBolus": None,
        "requestedBolus": REQUESTED_BOLUS_JSON,
        "nutrition": None,
        "food": json.dumps({"nutrition": {"carbohydrate": {"net": 34.0}}}),
        "originalFood": json.dumps({"nutrition": {"carbohydrate": {"net": 34.0}}}),
    }


def _make_food_row(user_id, day_start):
    ts = day_start + timedelta(hours=8)
    return {
        "_userId": user_id,
        "time": _time_struct(ts),
        "type": "food",
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

for group in GROUPS:
    for _ in range(group["days"]):
        day_start = START + timedelta(days=day_offset)
        rows += _make_cbg_rows(USER_ID, day_start, group["counts"])
        rows += _make_loop_rec_rows(USER_ID, day_start, group["is_autobolus"])
        if group["has_correction_bolus"]:
            rows.append(_make_correction_bolus_row(USER_ID, day_start))
        if group["has_meal_bolus"]:
            rows.append(_make_meal_bolus_row(USER_ID, day_start))
        if group["has_food"]:
            rows.append(_make_food_row(USER_ID, day_start))
        day_offset += 1

# =============================================================================
# Write to Databricks
# =============================================================================

TABLE = "dev.closed_loop_rwd.test_closed_loop_device_data"

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

schema = StructType([
    StructField("_userId", StringType()),
    StructField("time", StructType([StructField("$date", StringType())])),
    StructField("type", StringType()),
    StructField("reason", StringType()),
    StructField("origin", StringType()),
    StructField("value", DoubleType()),
    StructField("recommendedBasal", DoubleType()),
    StructField("recommendedBolus", DoubleType()),
    StructField("requestedBolus", StringType()),
    StructField("nutrition", StringType()),
    StructField("food", StringType()),
    StructField("originalFood", StringType()),
])

spark_df = spark.createDataFrame(rows, schema=schema)
spark_df.write.saveAsTable(TABLE)

df = pd.DataFrame(rows)
print(f"Created {TABLE}: {len(df):,} rows, {day_offset} days")
print(f"  CBG rows:          {sum(1 for r in rows if r.get('type') == 'cbg'):,}")
print(f"  Loop recs:         {sum(1 for r in rows if r.get('reason') == 'loop'):,}")
print(f"  Dosing decisions:  {sum(1 for r in rows if r.get('type') == 'dosingDecision'):,}")
print(f"  Food rows:         {sum(1 for r in rows if r.get('type') == 'food'):,}")
print()
for group in GROUPS:
    print(f"  {group['name']:30s}  {group['days']} days")
