"""The extreme-preset cohort tables of IR-6 (execution plan Step 3, 2026-11-05).

FDA IR6 asks for whole-window CGM outcomes of the users who activated presets
at the extremes of the to-be-marketed guardrail envelope. Until 2026-11-05
analysis IR-6 derived the category flags and computed the whole-segment
endpoints itself; both now live here, in the shape of the other cohort
tables, and the analysis reads them through the loaders.

Two tables:

  extreme_preset_activations   one row per preset activation of a rank-1
      window in the preset cohort (transition_activation_cohort
      .window_in_preset_cohort, any of the three periods), with the
      activation's parameters, its insulin-needs fraction (the basal factor,
      else 1/CR, else 1/ISF), the eight category flags, any_extreme, and the
      user's diagnosis group.

  extreme_preset_cohort        one row per (user, period) for every user with
      an extreme activation anywhere in the window (fixed membership: the
      same users in every period), with the eight memberships (user level),
      the eight active flags (an extreme activation of that category in that
      period), the exposure hours per category and period, the period's
      whole-segment endpoints read from the shared glycemic_endpoints_transition
      table (all three periods since 2026-11-06, one computation for the §8
      analyses and IR-6), the CGM count and the coverage floor, and the
      predicate

          in_extreme_cohort = any_extreme AND has_cgm AND passes_cgm_coverage

      the analysis selects on and the loader asserts.

The category bounds (locked 2026-08-19/20; the IR6 plan on Drive) are the
constants below; analysis IR-6 imports them from here.

Run (Databricks Run button):
  data_staging/export_extreme_preset_cohort.py
"""

import argparse
import os
import sys

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "data_staging",
    )
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from cohort_constants import MIN_CBG_COUNT  # noqa: E402

CATALOG = "dev.fda_510k_rwd"

# --- Extreme-preset definitions (locked 2026-08-19/20; the IR6 plan) --------
# Insulin needs at or beyond the TL 2.0 guardrail bounds.
NEEDS_LOW_MAX = 0.15    # needs <= 15%
NEEDS_HIGH_MIN = 2.0    # needs >= 200%
# Target bands with the +/- 0.5 mg/dL mmol round-trip tolerance (staged mg/dL
# values are mmol/L * 18.018, so nominal integers land an epsilon off).
TARGET_LOW_BAND_HIGH_MAX = 100.5   # own target high <= 100 -> low band
TARGET_HIGH_BAND_LOW_MIN = 179.5   # own target low >= 180 -> high band

# The category keys, the IR6 meeting order of 2026-08-20: the needs
# marginals (the analytic core), the target marginals, then the grid cells
# with the key safety cell leading. The first four are the marginals.
CATEGORY_KEYS = (
    "needs_le_15pct",
    "needs_ge_200pct",
    "target_low_band",
    "target_high_band",
    "needs_high_x_target_low",
    "needs_low_x_target_low",
    "needs_low_x_target_high",
    "needs_high_x_target_high",
)
N_MARGINALS = 4

# The three periods of a window as the activation table labels them, with
# the segment-date columns of valid_transition_segments that bound each.
SEGMENTS = (
    ("tb_to_ab_seg1", "tb_to_ab_seg1_start", "tb_to_ab_seg1_end"),
    ("tb_to_ab_seg2", "tb_to_ab_seg2_start", "tb_to_ab_seg2_end"),
    ("tb_to_ab_seg3", "tb_to_ab_seg3_start", "tb_to_ab_seg3_end"),
)

ENDPOINT_COLUMNS = ("cbg_count", "tbr_very_low", "tbr", "tir", "tar", "tar_very_high",
                    "mean_glucose", "cv", "hypo_events")


def _category_sql(prefix=""):
    """The eight category flags from the needs fraction and the target bounds."""
    low = f"({prefix}needs_frac <= {NEEDS_LOW_MAX})"
    high = f"({prefix}needs_frac >= {NEEDS_HIGH_MIN})"
    target_low = f"({prefix}bth <= {TARGET_LOW_BAND_HIGH_MAX})"
    target_high = f"({prefix}btl >= {TARGET_HIGH_BAND_LOW_MIN})"
    return {
        "needs_le_15pct": low,
        "needs_ge_200pct": high,
        "target_low_band": target_low,
        "target_high_band": target_high,
        "needs_high_x_target_low": f"({high} AND {target_low})",
        "needs_low_x_target_low": f"({low} AND {target_low})",
        "needs_low_x_target_high": f"({low} AND {target_high})",
        "needs_high_x_target_high": f"({high} AND {target_high})",
    }


def run(
    spark,
    activations_table=f"{CATALOG}.extreme_preset_activations",
    output_table=f"{CATALOG}.extreme_preset_cohort",
    activation_cohort_table=f"{CATALOG}.transition_activation_cohort",
    overrides_table=f"{CATALOG}.overrides_by_segment",
    segments_table=f"{CATALOG}.valid_transition_segments",
    endpoints_table=f"{CATALOG}.glycemic_endpoints_transition",
):
    categories = _category_sql()
    flag_columns = ",\n        ".join(f"COALESCE({sql}, FALSE) AS {key}" for key, sql in categories.items())
    any_extreme = " OR ".join(f"COALESCE({sql}, FALSE)" for sql in categories.values())

    # 1. Every activation of a preset-cohort window with its category flags.
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {activations_table} AS
    WITH parameters AS (
      SELECT
        a._userId,
        a.override_time,
        a.tb_to_ab_seg1_start,
        a.segment,
        a.diagnosis_group,
        a.is_primary_diagnosis,
        CAST(o.duration AS DOUBLE)                          AS duration,
        TRY_CAST(o.basalRateScaleFactor AS DOUBLE)          AS brsf,
        TRY_CAST(o.bg_target_low AS DOUBLE)                 AS btl,
        TRY_CAST(o.bg_target_high AS DOUBLE)                AS bth,
        TRY_CAST(o.carbRatioScaleFactor AS DOUBLE)          AS crsf,
        TRY_CAST(o.insulinSensitivityScaleFactor AS DOUBLE) AS issf
      FROM {activation_cohort_table} a
      JOIN {overrides_table} o
        ON a._userId = o._userId AND a.override_time = o.override_time
      WHERE a.window_in_preset_cohort
    ),
    needs AS (
      SELECT
        *,
        -- the derive_insulin_needs convention: the basal factor, else 1/CR,
        -- else 1/ISF, each guarded positive
        COALESCE(
          CASE WHEN brsf > 0 THEN brsf END,
          CASE WHEN crsf > 0 THEN 1.0 / crsf END,
          CASE WHEN issf > 0 THEN 1.0 / issf END
        ) AS needs_frac
      FROM parameters
    )
    SELECT
      *,
      {flag_columns},
      ({any_extreme}) AS any_extreme
    FROM needs
    ;
    """)

    # 2. The member users' three periods, with the window dates.
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TEMP VIEW epc_member_windows AS
    SELECT DISTINCT
      m._userId,
      m.tb_to_ab_seg1_start,
      m.diagnosis_group,
      m.is_primary_diagnosis,
      s.tb_to_ab_seg1_end, s.tb_to_ab_seg2_start, s.tb_to_ab_seg2_end,
      s.tb_to_ab_seg3_start, s.tb_to_ab_seg3_end
    FROM (SELECT DISTINCT _userId, tb_to_ab_seg1_start, diagnosis_group, is_primary_diagnosis
          FROM {activations_table} WHERE any_extreme) m
    JOIN {segments_table} s
      ON m._userId = s._userId AND m.tb_to_ab_seg1_start = s.tb_to_ab_seg1_start
    ;
    """)

    # 3. One row per (member user, period): memberships, active flags,
    # exposure, the whole-period endpoints from the shared transition
    # endpoints table (all three periods since 2026-11-06), the coverage
    # floor, and the predicate.
    member_columns = ",\n      ".join(
        f"MAX(CASE WHEN {key} THEN 1 ELSE 0 END) = 1 AS member_{key}" for key in CATEGORY_KEYS
    )
    active_columns = ",\n      ".join(
        f"COALESCE(MAX(CASE WHEN a.{key} THEN 1 ELSE 0 END) = 1, FALSE) AS active_{key}" for key in CATEGORY_KEYS
    )
    exposure_columns = ",\n      ".join(
        f"COALESCE(SUM(CASE WHEN a.{key} THEN a.duration ELSE 0 END) / 3600.0, 0.0) AS exposure_hours_{key}"
        for key in CATEGORY_KEYS
    )
    segment_rows = ", ".join(f"('{segment}')" for segment, _, _ in SEGMENTS)
    endpoint_columns = ",\n      ".join(f"e.{column}" for column in ENDPOINT_COLUMNS)

    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {output_table} AS
    WITH memberships AS (
      SELECT
        _userId,
        {member_columns}
      FROM {activations_table}
      WHERE any_extreme
      GROUP BY _userId
    ),
    user_segments AS (
      SELECT w._userId, w.tb_to_ab_seg1_start, w.diagnosis_group, w.is_primary_diagnosis, p.segment
      FROM epc_member_windows w
      CROSS JOIN (SELECT col1 AS segment FROM VALUES {segment_rows}) p
    ),
    actives AS (
      SELECT
        us._userId, us.segment,
        {active_columns},
        {exposure_columns}
      FROM user_segments us
      LEFT JOIN {activations_table} a
        ON a._userId = us._userId AND a.segment = us.segment AND a.any_extreme
      GROUP BY us._userId, us.segment
    )
    SELECT
      us._userId,
      us.segment,
      us.tb_to_ab_seg1_start,
      us.diagnosis_group,
      us.is_primary_diagnosis,
      TRUE AS any_extreme,
      m.* EXCEPT (_userId),
      ac.* EXCEPT (_userId, segment),
      e._userId IS NOT NULL AS has_cgm,
      {endpoint_columns},
      COALESCE(e.cbg_count, 0) >= {MIN_CBG_COUNT} AS passes_cgm_coverage,
      e._userId IS NOT NULL AND COALESCE(e.cbg_count, 0) >= {MIN_CBG_COUNT} AS in_extreme_cohort
    FROM user_segments us
    JOIN memberships m ON us._userId = m._userId
    JOIN actives ac ON us._userId = ac._userId AND us.segment = ac.segment
    LEFT JOIN {endpoints_table} e
      ON us._userId = e._userId
     AND us.tb_to_ab_seg1_start = e.tb_to_ab_seg1_start
     AND us.segment = e.segment
    ;
    """)
    spark.sql("DROP VIEW IF EXISTS epc_member_windows")
    print(f"[extreme-preset cohort] {activations_table} and {output_table} written")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--activations_table", default=f"{CATALOG}.extreme_preset_activations")
    _parser.add_argument("--output_table", default=f"{CATALOG}.extreme_preset_cohort")
    _args, _ = _parser.parse_known_args()
    run(spark, activations_table=_args.activations_table, output_table=_args.output_table)
