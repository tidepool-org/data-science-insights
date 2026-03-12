"""
Create a test_loop_data table mimicking bddp_sample_all with two users:
  - user_transition: 14 days temp basal → 14 days autobolus
  - user_no_transition: 28 days temp basal only

Columns match the format consumed by:
  - export_loop_recommendations.sql  (reason='loop', time, origin, recommendedBasal, recommendedBolus)
  - export_cbg_from_loop.sql         (type='cbg', time, value)
"""

import json
from datetime import datetime, timedelta

import pandas as pd

# --- Config ---
START = datetime(2025, 1, 1)
DAYS = 28
INTERVAL_MIN = 5
INTERVALS_PER_DAY = 288
ORIGIN = json.dumps({"version": "3.2.0"})
MMOL_PER_MGDL = 1 / 18.018  # value column is mmol/L

USER_TRANSITION = "test_user_transition"
USER_NO_TRANSITION = "test_user_no_transition"


def _timestamps(start, days):
    """Generate 5-min timestamps for `days` days starting at `start`."""
    return [start + timedelta(minutes=INTERVAL_MIN * i) for i in range(days * INTERVALS_PER_DAY)]


def _time_json(ts):
    """Format a timestamp as the JSON column bddp_sample_all expects."""
    return json.dumps({"$date": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")})


def _make_recommendation_rows(user_id, timestamps, is_autobolus_fn):
    """
    Create Loop recommendation rows.

    is_autobolus_fn(ts) -> bool determines whether each timestamp
    gets a recommendedBolus (autobolus) or recommendedBasal only (temp basal).
    """
    rows = []
    for ts in timestamps:
        autobolus = is_autobolus_fn(ts)
        rows.append({
            "_userId": user_id,
            "time": _time_json(ts),
            "type": None,
            "reason": "loop",
            "value": None,
            "origin": ORIGIN,
            "recommendedBasal": None if autobolus else 1.0,
            "recommendedBolus": 0.5 if autobolus else None,
        })
    return rows


def _make_cbg_rows(user_id, timestamps, glucose_mg_dl=120.0):
    """Create CBG rows with a constant glucose value."""
    rows = []
    for ts in timestamps:
        rows.append({
            "_userId": user_id,
            "time": _time_json(ts),
            "type": "cbg",
            "reason": "loop",
            "value": glucose_mg_dl * MMOL_PER_MGDL,
            "origin": None,
            "recommendedBasal": None,
            "recommendedBolus": None,
        })
    return rows


# --- Build rows ---
timestamps = _timestamps(START, DAYS)
transition_day = START + timedelta(days=14)

rows = []

# User A: temp basal days 1-14, autobolus days 15-28
rows += _make_recommendation_rows(
    USER_TRANSITION, timestamps,
    is_autobolus_fn=lambda ts: ts >= transition_day,
)
rows += _make_cbg_rows(USER_TRANSITION, timestamps)

# User B: temp basal all 28 days
rows += _make_recommendation_rows(
    USER_NO_TRANSITION, timestamps,
    is_autobolus_fn=lambda ts: False,
)
rows += _make_cbg_rows(USER_NO_TRANSITION, timestamps)

# --- Write table ---
df = pd.DataFrame(rows)
spark_df = spark.createDataFrame(df)  # type: ignore[name-defined]
spark_df.write.mode("overwrite").saveAsTable("dev.fda_510k_rwd.test_loop_data")

print(f"Created test_loop_data: {len(df)} rows")
print(f"  {USER_TRANSITION}: {len(df[df['_userId'] == USER_TRANSITION])} rows")
print(f"  {USER_NO_TRANSITION}: {len(df[df['_userId'] == USER_NO_TRANSITION])} rows")
