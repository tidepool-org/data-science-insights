"""Table 6.3a — Cohort Flow of the TB→AB transition cohort (RPT-1001 §6.3).

Stage-by-stage funnel from the BDDP sample down to the final transition
cohort, for the production build (suffix "") or a parallel validity-box
build (e.g. --suffix _box080; see exploratory/run_transition_variant.py).
Requested by the RPT-1001 report editor (developer_note.md, 2026-06-12) for
the box080-primary report copy.

Stage sources:
- The upstream stages (BDDP sample → Loop automated-dosing days → candidate
  28-day windows → day-coverage gate) are box-independent — the variant
  driver "branches from the box", reusing production upstream tables — so
  they are re-derived here with the same sliding-window logic as
  data_staging/export_valid_transition_segments.py and are identical for
  every build.
- The validity-box stage is read from the staged
  valid_transition_segments{suffix} table, so this script never needs the
  box thresholds the build was staged with.
- The analysis-side stages come from load_transition_endpoints(funnel=...) —
  the same code path the §8 analyses use, so the final row matches their
  cohort N exactly.

Run:
  analysis/analysis_6-3a_cohort_flow.py                    # production (0.70 box)
  analysis/analysis_6-3a_cohort_flow.py --suffix _box080   # 0.80-box build
"""

import argparse
import os

import pandas as pd

from utils.data_loading import CATALOG, load_transition_endpoints

OUTPUT_DIR = "outputs/cohort_6_3"

# Box-independent upstream sources, shared by every build (no {suffix}).
BDDP_TABLE = "dev.default.bddp_sample_all_2"
LOOP_RECOMMENDATIONS_TABLE = f"{CATALOG}.loop_recommendations"

# Candidate-window geometry + day-coverage gate, mirroring the `params` CTE in
# data_staging/export_valid_transition_segments.py: 14-day halves, ≥70% of
# days observed in each half.
SEGMENT_DAYS = 14
MIN_DAY_COVERAGE = 0.70


def _count(spark, sql):
    """Run a single-row counting query and return the row."""
    return spark.sql(sql).collect()[0]


def build_upstream_funnel(spark, suffix: str = "") -> list:
    """Funnel stages upstream of the analysis loaders, counted in SQL.

    Stages 1–4 are box-independent; stage 5 (the validity box + staging age
    gate) is read from the staged segments table for the requested build.
    """
    rows = []

    n_bddp = _count(
        spark, f"SELECT COUNT(DISTINCT _userId) AS n FROM {BDDP_TABLE}"
    )["n"]
    rows.append({
        "stage": "BDDP sample",
        "description": "Users in the BDDP research sample",
        "n_users": n_bddp,
        "n_segments": None,
    })

    n_loop = _count(
        spark,
        f"SELECT COUNT(DISTINCT _userId) AS n FROM {LOOP_RECOMMENDATIONS_TABLE}",
    )["n"]
    rows.append({
        "stage": "Loop automated dosing observed",
        "description": "≥1 day with a Loop-attributed automated bolus or "
                       "temp-basal (loop_recommendations)",
        "n_users": n_loop,
        "n_segments": None,
    })

    # Sliding-window candidacy + day-coverage, mirroring the `sliding_window`
    # and `scored` CTEs of export_valid_transition_segments.py (window
    # geometry, observation-bounds check, and coverage gate verbatim).
    windows = _count(spark, f"""
--begin-sql
WITH user_bounds AS (
  SELECT _userId, MIN(day) AS first_day
  FROM {LOOP_RECOMMENDATIONS_TABLE}
  GROUP BY _userId
),

sliding_window AS (
  SELECT
    r._userId,
    COUNT(*) OVER seg1 AS total_days_seg1,
    COUNT(*) OVER seg2 AS total_days_seg2,
    DATE_SUB(r.day, 27) >= b.first_day AS window_within_observation
  FROM {LOOP_RECOMMENDATIONS_TABLE} r
  JOIN user_bounds b ON r._userId = b._userId
  WINDOW
    seg1 AS (
      PARTITION BY r._userId
      ORDER BY r.day
      RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND INTERVAL 14 DAYS PRECEDING
    ),
    seg2 AS (
      PARTITION BY r._userId
      ORDER BY r.day
      RANGE BETWEEN INTERVAL 13 DAYS PRECEDING AND CURRENT ROW
    )
)

SELECT
  COUNT(DISTINCT CASE
    WHEN total_days_seg1 > 0
     AND total_days_seg2 > 0
     AND window_within_observation
    THEN _userId END) AS n_candidate,
  COUNT(DISTINCT CASE
    WHEN total_days_seg1 > 0
     AND total_days_seg2 > 0
     AND window_within_observation
     AND total_days_seg1 * 1.0 / {SEGMENT_DAYS} >= {MIN_DAY_COVERAGE}
     AND total_days_seg2 * 1.0 / {SEGMENT_DAYS} >= {MIN_DAY_COVERAGE}
    THEN _userId END) AS n_covered
FROM sliding_window
;
""")
    rows.append({
        "stage": "Candidate 28-day window",
        "description": f"≥1 window with both {SEGMENT_DAYS}-day halves "
                       f"observed, starting on/after the user's first "
                       f"observed day",
        "n_users": windows["n_candidate"],
        "n_segments": None,
    })
    rows.append({
        "stage": "Day-coverage gate",
        "description": f"≥ {MIN_DAY_COVERAGE:.0%} of days observed in each "
                       f"{SEGMENT_DAYS}-day half",
        "n_users": windows["n_covered"],
        "n_segments": None,
    })

    segments = _count(spark, f"""
--begin-sql
SELECT
  COUNT(DISTINCT _userId) AS n_users,
  COUNT(DISTINCT _userId, tb_to_ab_seg1_start) AS n_segments
FROM {CATALOG}.valid_transition_segments{suffix}
;
""")
    rows.append({
        "stage": "Valid TB→AB transition segment",
        "description": "Passes the build's segment-validity box (seg1 "
                       "temp-basal-dominated, seg2 autobolus-dominated); "
                       "staging age gate (> 6 y at segment start or DOB "
                       "unknown)",
        "n_users": segments["n_users"],
        "n_segments": segments["n_segments"],
    })

    return rows


def create_table_6_3a(spark, output_dir: str, suffix: str = ""):
    """Assemble the full funnel and write table_6_3a_cohort_flow.csv."""
    rows = build_upstream_funnel(spark, suffix=suffix)

    funnel = []
    wide = load_transition_endpoints(spark, suffix=suffix, funnel=funnel)
    rows.extend(funnel)

    table = pd.DataFrame(
        rows, columns=["stage", "description", "n_users", "n_segments"]
    )
    table["n_segments"] = table["n_segments"].astype("Int64")

    # The funnel only ever narrows: any increase means the re-derived upstream
    # stages have drifted from the staging SQL (or the loader changed order).
    users = table["n_users"].tolist()
    for prev, curr, stage in zip(users, users[1:], table["stage"][1:]):
        assert curr <= prev, (
            f"funnel is not monotone at stage `{stage}`: {curr} > {prev}"
        )

    out = f"{output_dir}/table_6_3a_cohort_flow.csv"
    table.to_csv(out, index=False)
    print(f"  Saved: {out}")
    return table, wide


def run_analysis(spark, output_dir=None, suffix: str = ""):
    # suffix='_box080' runs on the parallel 0.80-box cohort and writes to a
    # parallel output dir so the production outputs aren't clobbered.
    if output_dir is None:
        output_dir = OUTPUT_DIR + suffix
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Table 6.3a: Cohort Flow (TB→AB transition cohort)")
    print("=" * 60)

    print(f"\n1. Building funnel (suffix={suffix!r})...")
    table, wide = create_table_6_3a(spark, output_dir, suffix=suffix)
    print("\n" + table.to_string(index=False))

    print("\n" + "=" * 60)
    print("Table 6.3a Complete!")
    print(f"Outputs saved to: {output_dir}/")
    print("=" * 60)

    return {"table_6_3a": table, "df": wide}


def run_in_databricks(spark, suffix: str = ""):
    return run_analysis(spark, suffix=suffix)


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default="",
                         help="source-table suffix, e.g. _box080 for the 0.80-box cohort")
    _args, _ = _parser.parse_known_args()
    run_in_databricks(spark, suffix=_args.suffix)  # type: ignore[name-defined]
