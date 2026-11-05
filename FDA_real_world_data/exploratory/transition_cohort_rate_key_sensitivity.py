"""Table 8.1a with and without the users whose temp-basal period rests on the rate-key basal rule.

MC, 2026-11-04, on the first §8-1 run of the rebuilt tables (a far larger
type-1 cohort than Rev 02's, the time-in-range difference about half its Rev
02 size): "Can you compare if we don't include the rate users". The rate
rule is the classifier's health_rate_loop_app tag (an Apple Health basal with
Loop's programmed temporary-basal rate key, from Loop's app identifier,
whatever the automatic key says), the tag behind most of the user-days the
old day table had no row for (Gate B result 0).

For each user of the cohort, the selected window's first period is re-read
from the dose table with the rate-key tag withheld: a day stays a temp-basal
day only if a temporary basal that another rule recognised is on it. A
window is rate-key dependent when the first period then falls short of the
12 of 14 rule, so the user would not be in the cohort under a classifier
without that tag. The re-read also reproduces the window's temp-basal day
count with every tag, which is asserted equal to the staged count (the day
table is the dose table's projection).

Alongside, two splits that bear on the same question: membership of the Rev
02 cohort (the loader on the 2026-10-29 vintage copies of the 0.80-box
tables, the run RPT-1001 Rev 02 read), and the first period's dose profile
(automated and user boluses per day, user bolus size, temporary basals per
day, upload route, Loop version by period). Autobolus dosing delivers tens
of small automated boluses a day and temp-basal dosing none, so a "temp
basal" period with autobolus-like bolus counts would be a classification
miss, which must be ruled out before any reading of the smaller difference.

Outputs (aggregates only, no user identifiers), under
analysis/outputs/transition_cohort_rate_key_sensitivity/:
  result_users_by_subset.csv              users per diagnosis group, rate-key
                                          dependence, and Rev 02 membership
  result_table_8_1a_parametric_by_subset.csv, result_table_8_1a_nonparametric_by_subset.csv
                                          Table 8.1a of the primary cohort per
                                          subset (a subset under 5 users: N only)
  result_first_period_dose_profile_by_subset.csv
  result_rate_key_only_basals_by_automatic_key.csv
  result_loop_version_state_by_subset.csv
  manifest.csv                            run time, Spark version, script SHA-256

And, under analysis/outputs/analysis_8_1_without_rate_key_dependent/ (MC,
2026-11-05: "I need the non rate dep cohort with the lada highlighted and
separately reported"), the full §8-1 output set of analysis 8-1 for the
cohort without the rate-key-dependent windows: Table 8.1a and Figures 8.1a to
8.1c on the primary groups, and the by-diagnosis-group appendix (the users per
group, Table 8.1a per series, Figures 8.1a and 8.1c with the LADA series
beside type 1), every figure titled with the cohort, plus a copy of the
manifest. The same files as the production run's, so they compare one to one.

Run on Databricks (Run button), after production_runs/run_production_staging.py.
"""

import argparse
import datetime as dt
import hashlib
import importlib.util
import os
import sys

import numpy as np
import pandas as pd

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Databricks notebook-view of a .py file doesn't define __file__.
    _HERE = os.path.join(
        os.environ.get(
            "FDA_RWD_ROOT",
            "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/FDA_real_world_data",
        ),
        "exploratory",
    )
_FDA_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_FDA_ROOT, "analysis"))
sys.path.insert(0, os.path.join(_FDA_ROOT, "data_staging"))

from export_loop_recommendations import (  # noqa: E402
    AUTOBOLUS_DAY_MIN_BOLUSES, BASAL_TAGS,
    UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS, UNRECOGNISED_AUTOMATED_MIN_BOLUSES,
)
from export_valid_transition_segments import DEFAULT_MIN_PERIOD_DAYS, SEGMENT_DAYS  # noqa: E402
from utils.data_loading import CATALOG, PRIMARY_DIAGNOSIS_GROUPS, load_transition_endpoints  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "analysis_8_1",
    os.path.join(_FDA_ROOT, "analysis",
                 "analysis_8-1_comparative_clinical_performance_and_safety_of_autobolus_vs_temporary_basal_dosing_strategies.py"))
analysis_8_1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(analysis_8_1)

DOSE_TABLE = f"{CATALOG}.loop_dose_classification"
SEGMENTS_TABLE = f"{CATALOG}.valid_transition_segments"
# The vintage of record for RPT-1001 Rev 02: the 0.80-box tables as copied on
# 2026-10-29 (the loader builds valid_transition_segments{suffix} etc.).
REV02_SUFFIX = "_box080_vintage_2026_10_29"
RATE_KEY_TAG = "health_rate_loop_app"
# Temporary-basal rules other than the rate key, from the staging's own list.
OTHER_BASAL_TAGS = tuple(tag for tag in BASAL_TAGS if tag != RATE_KEY_TAG)
MIN_USERS_PER_SUBSET = analysis_8_1.MIN_USERS_PER_SERIES
OUTPUT_DIR = os.path.join(_FDA_ROOT, "analysis", "outputs", "transition_cohort_rate_key_sensitivity")
# The §8-1 output set for the cohort without the rate-key-dependent windows,
# a sibling of analysis 8-1's production folder with the same file names.
WITHOUT_RATE_KEY_OUTPUT_DIR = os.path.join(_FDA_ROOT, "analysis", "outputs", "analysis_8_1_without_rate_key_dependent")
# The last line of every figure title in that folder.
WITHOUT_RATE_KEY_COHORT_NOTE = "Cohort without rate-key-dependent windows (transition_cohort_rate_key_sensitivity.py)"

LOOP3_VERSION_INT = 3_000_000


def window_composition(spark, windows: pd.DataFrame) -> pd.DataFrame:
    """One row per cohort window: its day counts with and without the rate-key tag, and its dose profile.

    `windows` carries `_userId` and `tb_to_ab_seg1_start`. The first period is
    the 14 days from seg1_start, the second the 14 after it. Day classes are
    restated here from the dose table on purpose: the whole point is to
    withhold one tag, which the day table cannot do.
    """
    other_rule = "GREATEST(" + ", ".join(f"d.{tag}" for tag in OTHER_BASAL_TAGS) + ")"
    spark.createDataFrame(windows[["_userId", "tb_to_ab_seg1_start"]]).createOrReplaceTempView("cohort_windows")
    return spark.sql(f"""
    --begin-sql
    WITH windows AS (
      SELECT _userId,
             tb_to_ab_seg1_start                                    AS seg1_start,
             DATE_ADD(tb_to_ab_seg1_start, {SEGMENT_DAYS} - 1)     AS seg1_end,
             DATE_ADD(tb_to_ab_seg1_start, 2 * {SEGMENT_DAYS} - 1) AS seg2_end
      FROM cohort_windows
    ),

    -- every dose of the window, with the period it falls in and whether a
    -- temporary basal would still be recognised without the rate-key tag
    doses AS (
      SELECT w._userId, w.seg1_start,
             CASE WHEN d.day <= w.seg1_end THEN 1 ELSE 2 END                         AS period,
             d.day, d.type, d.units, d.routes, d.is_automated, d.automatic_key, d.version_int,
             CASE WHEN d.type = 'basal' AND d.is_automated = 1 AND {other_rule} = 1
                  THEN 1 ELSE 0 END                                                  AS temp_basal_other_rule,
             CASE WHEN d.type = 'basal' AND d.is_automated = 1 AND {other_rule} = 0
                  THEN 1 ELSE 0 END                                                  AS temp_basal_rate_key_only
      FROM windows w
      JOIN {DOSE_TABLE} d
        ON d._userId = w._userId AND d.day BETWEEN w.seg1_start AND w.seg2_end
    ),

    days AS (
      SELECT _userId, seg1_start, period, day,
             SUM(CASE WHEN type = 'bolus' AND is_automated = 1 THEN 1 ELSE 0 END)  AS automated_boluses,
             SUM(CASE WHEN type = 'bolus' AND is_automated = 0 THEN 1 ELSE 0 END)  AS user_boluses,
             SUM(CASE WHEN type = 'basal' AND is_automated = 1 THEN 1 ELSE 0 END)  AS temp_basals,
             SUM(temp_basal_other_rule)                                             AS temp_basals_other_rule,
             SUM(temp_basal_rate_key_only)                                          AS temp_basals_rate_key_only,
             SUM(CASE WHEN routes LIKE '%plugin%' THEN 1 ELSE 0 END)                AS doses_with_plugin_copy,
             COUNT(*)                                                               AS doses,
             MAX(version_int)                                                       AS version_int,
             -- the day rule's bar: unrecognised boluses that look automated
             -- (the staging's constants) keep a day from being a temp-basal day
             CASE WHEN SUM(CASE WHEN type = 'bolus' AND is_automated = 0 THEN 1 ELSE 0 END)
                       >= {UNRECOGNISED_AUTOMATED_MIN_BOLUSES}
                   AND MEDIAN(CASE WHEN type = 'bolus' AND is_automated = 0 THEN units END)
                       < {UNRECOGNISED_AUTOMATED_MAX_MEDIAN_UNITS}
                  THEN 1 ELSE 0 END                                                 AS user_boluses_look_automated
      FROM doses
      GROUP BY _userId, seg1_start, period, day
    ),

    user_bolus_size AS (
      SELECT _userId, seg1_start,
             PERCENTILE_APPROX(units, 0.5) AS user_bolus_median_units_seg1
      FROM doses
      WHERE period = 1 AND type = 'bolus' AND is_automated = 0
      GROUP BY _userId, seg1_start
    ),

    rate_key_only_keys AS (
      SELECT _userId, seg1_start,
             SUM(CASE WHEN automatic_key = '1'      THEN 1 ELSE 0 END) AS rate_key_only_basals_key_1,
             SUM(CASE WHEN automatic_key = '0'      THEN 1 ELSE 0 END) AS rate_key_only_basals_key_0,
             SUM(CASE WHEN automatic_key = 'other'  THEN 1 ELSE 0 END) AS rate_key_only_basals_key_other,
             SUM(CASE WHEN automatic_key = 'absent' OR automatic_key IS NULL THEN 1 ELSE 0 END)
                                                                       AS rate_key_only_basals_key_absent
      FROM doses
      WHERE period = 1 AND temp_basal_rate_key_only = 1
      GROUP BY _userId, seg1_start
    )

    SELECT y._userId, y.seg1_start AS tb_to_ab_seg1_start,
           -- the first period's temp-basal days as the day table classes them,
           -- and the same count with the rate-key tag withheld
           SUM(CASE WHEN period = 1 AND automated_boluses < {AUTOBOLUS_DAY_MIN_BOLUSES} AND temp_basals > 0
                     AND user_boluses_look_automated = 0
                    THEN 1 ELSE 0 END)                                         AS temp_basal_days_seg1,
           SUM(CASE WHEN period = 1 AND automated_boluses < {AUTOBOLUS_DAY_MIN_BOLUSES} AND temp_basals_other_rule > 0
                     AND user_boluses_look_automated = 0
                    THEN 1 ELSE 0 END)                                         AS temp_basal_days_seg1_without_rate_key,
           SUM(CASE WHEN period = 2 AND automated_boluses >= {AUTOBOLUS_DAY_MIN_BOLUSES}
                    THEN 1 ELSE 0 END)                                         AS autobolus_days_seg2,
           -- the dose profile of each period, per calendar day of the period
           SUM(CASE WHEN period = 1 THEN automated_boluses ELSE 0 END) / {SEGMENT_DAYS} AS automated_boluses_per_day_seg1,
           SUM(CASE WHEN period = 1 THEN user_boluses      ELSE 0 END) / {SEGMENT_DAYS} AS user_boluses_per_day_seg1,
           SUM(CASE WHEN period = 1 THEN temp_basals       ELSE 0 END) / {SEGMENT_DAYS} AS temp_basals_per_day_seg1,
           SUM(CASE WHEN period = 1 THEN temp_basals_rate_key_only ELSE 0 END)
             / GREATEST(SUM(CASE WHEN period = 1 THEN temp_basals ELSE 0 END), 1)       AS rate_key_only_share_of_temp_basals_seg1,
           SUM(CASE WHEN period = 2 THEN automated_boluses ELSE 0 END) / {SEGMENT_DAYS} AS automated_boluses_per_day_seg2,
           SUM(CASE WHEN period = 2 THEN user_boluses      ELSE 0 END) / {SEGMENT_DAYS} AS user_boluses_per_day_seg2,
           SUM(CASE WHEN period = 1 THEN doses_with_plugin_copy ELSE 0 END)             AS plugin_doses_seg1,
           MAX(CASE WHEN period = 1 THEN version_int END)                                AS version_int_seg1,
           MAX(CASE WHEN period = 2 THEN version_int END)                                AS version_int_seg2,
           MAX(b.user_bolus_median_units_seg1)                                           AS user_bolus_median_units_seg1,
           COALESCE(MAX(k.rate_key_only_basals_key_1), 0)                                AS rate_key_only_basals_key_1,
           COALESCE(MAX(k.rate_key_only_basals_key_0), 0)                                AS rate_key_only_basals_key_0,
           COALESCE(MAX(k.rate_key_only_basals_key_other), 0)                            AS rate_key_only_basals_key_other,
           COALESCE(MAX(k.rate_key_only_basals_key_absent), 0)                           AS rate_key_only_basals_key_absent
    FROM days y
    LEFT JOIN user_bolus_size b  ON b._userId = y._userId AND b.seg1_start = y.seg1_start
    LEFT JOIN rate_key_only_keys k ON k._userId = y._userId AND k.seg1_start = y.seg1_start
    GROUP BY y._userId, y.seg1_start
    ;
    """).toPandas()


def version_state(version_int_seg1, version_int_seg2) -> str:
    """Where a Loop 3 version is read in the window: the plugin copies carry it, Apple Health alone does not."""
    first = pd.notna(version_int_seg1) and version_int_seg1 >= LOOP3_VERSION_INT
    second = pd.notna(version_int_seg2) and version_int_seg2 >= LOOP3_VERSION_INT
    if first and second:
        return "Loop 3 in both periods"
    if second:
        return "Loop 3 in the autobolus period only"
    if first:
        return "Loop 3 in the temp-basal period only"
    return "no version in the window"


def flag_windows(wide: pd.DataFrame, composition: pd.DataFrame, rev02: pd.DataFrame,
                 min_period_days=DEFAULT_MIN_PERIOD_DAYS) -> pd.DataFrame:
    """The cohort frame with the sensitivity flags on every user.

    rate_key_dependent     the first period passes 12 of 14 only with the rate-key tag
    in_rev02               the user is in the Rev 02 cohort (the loader on the vintage)
    same_window_as_rev02   and on the same window (the same first-period start)
    route_seg1             'plugin' when any first-period dose has a plugin copy
    version_state          see version_state()
    """
    frame = wide.merge(composition, on=["_userId", "tb_to_ab_seg1_start"], how="left", validate="one_to_one")
    missing = frame["temp_basal_days_seg1"].isna().sum()
    assert missing == 0, f"{missing} cohort windows have no doses in the dose table"
    # the day table is the dose table's projection, so the re-read with every
    # tag must give the staged window its temp-basal day count
    mismatch = (frame["staged_temp_basal_days_seg1"].astype(int) != frame["temp_basal_days_seg1"].astype(int)).sum()
    assert mismatch == 0, f"{mismatch} windows: the dose table's temp-basal day count differs from the staged one"
    frame["rate_key_dependent"] = frame["temp_basal_days_seg1_without_rate_key"] < min_period_days
    rev02_keys = rev02[["_userId", "tb_to_ab_seg1_start"]].drop_duplicates()
    frame["in_rev02"] = frame["_userId"].isin(rev02_keys["_userId"])
    same = pd.MultiIndex.from_frame(frame[["_userId", "tb_to_ab_seg1_start"]]).isin(pd.MultiIndex.from_frame(rev02_keys))
    frame["same_window_as_rev02"] = same
    frame["route_seg1"] = np.where(frame["plugin_doses_seg1"].fillna(0) > 0, "plugin", "apple_health only")
    frame["version_state"] = [version_state(a, b) for a, b in zip(frame["version_int_seg1"], frame["version_int_seg2"])]
    return frame


# (label, mask on the flagged primary-cohort frame)
SUBSETS = [
    ("primary cohort", lambda f: pd.Series(True, index=f.index)),
    ("without rate-key-dependent windows", lambda f: ~f["rate_key_dependent"]),
    ("rate-key-dependent windows", lambda f: f["rate_key_dependent"]),
    ("in the Rev 02 cohort", lambda f: f["in_rev02"]),
    ("in the Rev 02 cohort, same window", lambda f: f["same_window_as_rev02"]),
    ("not in the Rev 02 cohort", lambda f: ~f["in_rev02"]),
    ("not in the Rev 02 cohort, without rate-key-dependent windows", lambda f: ~f["in_rev02"] & ~f["rate_key_dependent"]),
]


def users_by_subset(frame: pd.DataFrame) -> pd.DataFrame:
    """Users per diagnosis group, rate-key dependence, and Rev 02 membership; every group listed."""
    counts = (frame.groupby(["diagnosis_group", "rate_key_dependent", "in_rev02"]).size()
              .rename("n_users").reset_index())
    total = pd.DataFrame([{"diagnosis_group": "all", "rate_key_dependent": r, "in_rev02": m,
                           "n_users": int(((frame["rate_key_dependent"] == r) & (frame["in_rev02"] == m)).sum())}
                          for r in (False, True) for m in (False, True)])
    return pd.concat([counts, total], ignore_index=True)


def table_8_1a_by_subset(primary: pd.DataFrame):
    """Table 8.1a (parametric and nonparametric) once per subset of the primary cohort."""
    parametric, nonparametric = [], []
    for label, mask in SUBSETS:
        frame = primary[mask(primary)]
        head = {"Subset": label, "Drawn": int(len(frame) >= MIN_USERS_PER_SUBSET)}
        if len(frame) >= MIN_USERS_PER_SUBSET:
            p, n = analysis_8_1.create_table_8_1a(frame)
            parametric.extend({**head, **row} for row in p.to_dict("records"))
            nonparametric.extend({**head, **row} for row in n.to_dict("records"))
        else:
            for name, c1, c2, _ in analysis_8_1.ENDPOINTS:
                valid = frame[c1].notna() & frame[c2].notna()
                parametric.append({**head, "Endpoint": name, "N": int(valid.sum())})
                nonparametric.append({**head, "Endpoint": name, "N": int(valid.sum())})
    return pd.DataFrame(parametric), pd.DataFrame(nonparametric)


PROFILE_COLUMNS = [
    "automated_boluses_per_day_seg1", "user_boluses_per_day_seg1", "user_bolus_median_units_seg1",
    "temp_basals_per_day_seg1", "rate_key_only_share_of_temp_basals_seg1",
    "automated_boluses_per_day_seg2", "user_boluses_per_day_seg2",
]


def dose_profile_by_subset(primary: pd.DataFrame) -> pd.DataFrame:
    """Per subset: the mean and median over users of each dose-profile measure, and the plugin-route share."""
    rows = []
    for label, mask in SUBSETS:
        frame = primary[mask(primary)]
        row = {"Subset": label, "N": len(frame),
               "share_with_plugin_copy_seg1": float((frame["route_seg1"] == "plugin").mean()) if len(frame) else np.nan}
        for column in PROFILE_COLUMNS:
            row[f"{column}_mean"] = float(frame[column].mean()) if len(frame) else np.nan
            row[f"{column}_median"] = float(frame[column].median()) if len(frame) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def rate_key_only_basals_by_automatic_key(primary: pd.DataFrame) -> pd.DataFrame:
    """Per subset: the first period's rate-key-only temporary basals by the automatic key on their Apple Health copy."""
    rows = []
    for label, mask in SUBSETS:
        frame = primary[mask(primary)]
        rows.append({"Subset": label, "N": len(frame),
                     **{key: int(frame[f"rate_key_only_basals_key_{key}"].sum())
                        for key in ("1", "0", "other", "absent")}})
    return pd.DataFrame(rows)


def version_state_by_subset(primary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for label, mask in SUBSETS:
        frame = primary[mask(primary)]
        counts = frame["version_state"].value_counts()
        rows.append({"Subset": label, "N": len(frame),
                     **{state: int(counts.get(state, 0)) for state in
                        ("Loop 3 in both periods", "Loop 3 in the autobolus period only",
                         "Loop 3 in the temp-basal period only", "no version in the window")}})
    return pd.DataFrame(rows)


def write_analysis_8_1_without_rate_key(flagged: pd.DataFrame, output_dir: str = WITHOUT_RATE_KEY_OUTPUT_DIR):
    """Analysis 8-1's full output set for the cohort without the rate-key-dependent windows.

    `flagged` is the cohort of every diagnosis group with the sensitivity
    flags (flag_windows). The users kept are those whose selected window
    passes 12 of 14 without the rate-key tag; the primary groups give Table
    8.1a and Figures 8.1a to 8.1c, every group gives the by-diagnosis-group
    appendix with the LADA series beside type 1 (MC, 2026-11-05).
    """
    kept = flagged[~flagged["rate_key_dependent"]].copy()
    primary = kept[kept["diagnosis_group"].isin(PRIMARY_DIAGNOSIS_GROUPS)].copy()
    print(f"   without rate-key-dependent windows: {len(kept)} users of every group, {len(primary)} primary")
    return analysis_8_1.write_outputs(primary, kept, output_dir, WITHOUT_RATE_KEY_COHORT_NOTE)


def write_manifest(spark, output_dir: str):
    script = os.path.join(_HERE, "transition_cohort_rate_key_sensitivity.py")
    sha = hashlib.sha256(open(script, "rb").read()).hexdigest() if os.path.exists(script) else ""
    pd.DataFrame([{
        "run_time_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "spark_version": spark.version,
        "script": "exploratory/transition_cohort_rate_key_sensitivity.py",
        "script_sha256": sha,
        "dose_table": DOSE_TABLE,
        "rev02_suffix": REV02_SUFFIX,
    }]).to_csv(os.path.join(output_dir, "manifest.csv"), index=False)


def run(spark, output_dir=OUTPUT_DIR, without_rate_key_output_dir=WITHOUT_RATE_KEY_OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)
    print("1. the cohort on the production tables, every diagnosis group")
    wide = load_transition_endpoints(spark, diagnosis_groups=None)
    print(f"   {len(wide)} users")
    print(f"2. the Rev 02 cohort: the loader on the vintage ({REV02_SUFFIX})")
    rev02 = load_transition_endpoints(spark, suffix=REV02_SUFFIX, diagnosis_groups=None)
    print(f"   {len(rev02)} users")
    print("3. each window's first period re-read from the dose table, with and without the rate-key tag")
    staged = (spark.table(SEGMENTS_TABLE)
              .selectExpr("_userId", "tb_to_ab_seg1_start", "tb_to_ab_temp_basal_days_seg1 AS staged_temp_basal_days_seg1")
              .toPandas())
    wide = wide.merge(staged, on=["_userId", "tb_to_ab_seg1_start"], how="left", validate="one_to_one")
    assert wide["staged_temp_basal_days_seg1"].notna().all(), "a cohort window missing from the segments table"
    composition = window_composition(spark, wide)
    flagged = flag_windows(wide, composition, rev02)
    primary = flagged[flagged["diagnosis_group"].isin(PRIMARY_DIAGNOSIS_GROUPS)].copy()
    print(f"   primary cohort {len(primary)}; rate-key dependent {int(primary['rate_key_dependent'].sum())}; "
          f"in Rev 02 {int(primary['in_rev02'].sum())} (same window {int(primary['same_window_as_rev02'].sum())})")

    print("4. writing the aggregate results")
    users_by_subset(flagged).to_csv(os.path.join(output_dir, "result_users_by_subset.csv"), index=False)
    parametric, nonparametric = table_8_1a_by_subset(primary)
    parametric.to_csv(os.path.join(output_dir, "result_table_8_1a_parametric_by_subset.csv"), index=False)
    nonparametric.to_csv(os.path.join(output_dir, "result_table_8_1a_nonparametric_by_subset.csv"), index=False)
    dose_profile_by_subset(primary).to_csv(
        os.path.join(output_dir, "result_first_period_dose_profile_by_subset.csv"), index=False)
    rate_key_only_basals_by_automatic_key(primary).to_csv(
        os.path.join(output_dir, "result_rate_key_only_basals_by_automatic_key.csv"), index=False)
    version_state_by_subset(primary).to_csv(
        os.path.join(output_dir, "result_loop_version_state_by_subset.csv"), index=False)
    write_manifest(spark, output_dir)
    print(parametric.to_string(index=False))

    print("5. analysis 8-1's output set for the cohort without the rate-key-dependent windows")
    without_rate_key = write_analysis_8_1_without_rate_key(flagged, without_rate_key_output_dir)
    write_manifest(spark, without_rate_key_output_dir)
    print(f"done: {output_dir} and {without_rate_key_output_dir}")
    return {"flagged": flagged, "parametric": parametric, "nonparametric": nonparametric,
            "without_rate_key": without_rate_key}


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_dir", default=OUTPUT_DIR)
    _parser.add_argument("--without_rate_key_output_dir", default=WITHOUT_RATE_KEY_OUTPUT_DIR)
    _args, _ = _parser.parse_known_args()
    run(spark, output_dir=_args.output_dir, without_rate_key_output_dir=_args.without_rate_key_output_dir)
