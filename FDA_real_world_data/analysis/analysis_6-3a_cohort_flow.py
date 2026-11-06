"""Table 6.3a and the cohort flow of every PLN-1001 population (RPT-1001 §6.3).

Execution plan 2026-10-27, Step 3 items 2 and 3. Each flow is counted from a
staged cohort table's criterion columns (utils/cohort_flow.py), so a stage
applies exactly the predicate its analysis applies, is split by diagnosis
group, and is written for the corrected vintage beside the issued one.

Written to outputs/cohort_6_3{suffix}/:

  table_6_3a_cohort_flow.csv
      The report's funnel, BDDP sample to the final transition cohort, in
      the issued Table 6.3a's twelve stages (the "Type 1 diabetes" stage
      narrows it to the primary cohort), with the issued vintage's counts
      and the difference beside the corrected ones where the issued cohort
      table exists (its upstream stages are not reproducible, since the
      issued day table is gone, and are left blank).
  table_6_3a_cohort_flow_by_diagnosis_group.csv
      The same stages without the type-1 stage, one column per diagnosis
      group plus the totals (all, primary).
  table_flow_transition_long.csv
      The long form behind both, every group and both vintages.
  table_flow_carbohydrates.csv
      Analysis 8-8's population: the transition stages then carbohydrate
      data in both periods, by group.
  table_flow_preset_activations_seg2.csv, _seg3.csv
      The preset-activation population (8-2, 8-3, 8-4, IR-1) in
      activations and users, by group, for each autobolus period pairing.
  table_flow_stable_autobolus.csv, table_flow_stable_autobolus_with_guardrail.csv
      Analysis 8-6's population (production build only), the second with
      the guardrail check 8-6 does not apply.
  table_flow_durability{coverage}.csv
      Analysis 8-7's population for each coverage build whose table exists
      (production build only).

The script's last act is Gate C's check: the final stage of Table 6.3a
equals the cohort an independent loader call returns.

Run:
  analysis/analysis_6-3a_cohort_flow.py                    # production build (12 of 14)
  analysis/analysis_6-3a_cohort_flow.py --suffix _box070   # a window-rule sensitivity build
"""

import argparse
import os
import shutil

import pandas as pd

from utils.cohort_flow import (
    ALL_GROUPS_LABEL,
    PRIMARY_LABEL,
    activation_stages,
    assert_monotone,
    carbohydrate_stages,
    count_flow,
    durability_stages,
    side_by_side,
    stable_stages,
    transition_stages,
    widen_flow,
)
from utils.data_loading import (
    ACTIVATION_COHORT_TABLE,
    CATALOG,
    DIAGNOSIS_GROUPS,
    DURABILITY_COHORT_TABLE,
    PRIMARY_DIAGNOSIS_GROUPS,
    STABLE_COHORT_TABLE,
    TRANSITION_COHORT_TABLE,
    load_transition_endpoints,
)

OUTPUT_DIR = "outputs/cohort_6_3"

# Box-independent upstream sources, shared by every build (no {suffix}).
BDDP_TABLE = "dev.default.bddp_sample_all_2"
LOOP_RECOMMENDATIONS_TABLE = f"{CATALOG}.loop_recommendations"
DIAGNOSIS_TABLE = f"{CATALOG}.user_diagnosis_type"

# The issued vintage's cohort table beside each build's (production_runs/
# run_production_staging.py, the issued_cohorts step): the `_box080` copies
# behind RPT-1001 Rev 02 for the primary, the 2026-08-05 supplement copies
# for the sensitivity builds.
ISSUED_COHORT_TABLE = "transition_cohort_issued{suffix}"

# The durability coverage builds whose flow is written with the production
# run (run_all_coverages.py's suffixes).
COVERAGE_SUFFIXES = ("", "_cov070", "_cov090")

# The three stages above the window search, counted in SQL on the day table.
UPSTREAM_STAGES = (
    ("BDDP sample", "Users in the BDDP research sample"),
    ("Loop automated dosing observed",
     "≥1 day with a Loop-attributed automated bolus or temp-basal (loop_recommendations)"),
    ("Candidate 28-day window",
     "≥1 window with both 14-day periods observed, starting on/after the user's first observed day"),
)


def _table_exists(spark, name):
    return spark.catalog.tableExists(name)


def _group_counts(spark, users_sql):
    """Users of a SQL statement (one `_userId` column) counted per diagnosis
    group through the lookup; a user absent from the lookup is 'unknown'."""
    counts = spark.sql(f"""
--begin-sql
SELECT COALESCE(d.diagnosis_group, 'unknown') AS diagnosis_group,
       COUNT(DISTINCT u._userId) AS n_users
FROM ({users_sql}) u
LEFT JOIN {DIAGNOSIS_TABLE} d ON u._userId = d._userId
GROUP BY COALESCE(d.diagnosis_group, 'unknown')
;
""").toPandas()
    by_group = dict(zip(counts["diagnosis_group"], counts["n_users"].astype(int)))
    rows = {ALL_GROUPS_LABEL: sum(by_group.values()),
            PRIMARY_LABEL: sum(by_group.get(g, 0) for g in PRIMARY_DIAGNOSIS_GROUPS)}
    rows.update({g: by_group.get(g, 0) for g in DIAGNOSIS_GROUPS})
    return rows


def upstream_flow(spark):
    """The three upstream stages as a long frame (stage, description, group,
    n_users, n_windows blank), mirroring the sliding-window geometry of
    data_staging/export_valid_transition_segments.py."""
    bddp_users = f"SELECT DISTINCT _userId FROM {BDDP_TABLE}"
    loop_users = f"SELECT DISTINCT _userId FROM {LOOP_RECOMMENDATIONS_TABLE}"
    candidate_users = f"""
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
    seg1 AS (PARTITION BY r._userId ORDER BY r.day
             RANGE BETWEEN INTERVAL 27 DAYS PRECEDING AND INTERVAL 14 DAYS PRECEDING),
    seg2 AS (PARTITION BY r._userId ORDER BY r.day
             RANGE BETWEEN INTERVAL 13 DAYS PRECEDING AND CURRENT ROW)
)
SELECT DISTINCT _userId
FROM sliding_window
WHERE total_days_seg1 > 0 AND total_days_seg2 > 0 AND window_within_observation"""
    rows = []
    for (stage, description), sql in zip(UPSTREAM_STAGES, (bddp_users, loop_users, candidate_users)):
        for group, n_users in _group_counts(spark, sql).items():
            rows.append({"stage": stage, "description": description, "group": group,
                         "n_users": n_users, "n_windows": pd.NA})
    return pd.DataFrame(rows, columns=["stage", "description", "group", "n_users", "n_windows"])


def transition_flows(spark, suffix, issued_table):
    """The corrected build's transition flow (upstream stages included) and
    the issued vintage's (window stages only), both long, both with and
    without the type-1 stage."""
    cohort = spark.table(f"{CATALOG}.{TRANSITION_COHORT_TABLE.format(suffix=suffix)}").toPandas()
    upstream = upstream_flow(spark)
    current = {
        "report": pd.concat([upstream, count_flow(cohort, transition_stages(primary_gate=True), unit="windows")],
                            ignore_index=True),
        "by_group": pd.concat([upstream, count_flow(cohort, transition_stages(), unit="windows")],
                              ignore_index=True),
        "carbs": count_flow(cohort, carbohydrate_stages(), unit="windows"),
    }
    issued = None
    if issued_table is not None and _table_exists(spark, issued_table):
        issued_cohort = spark.table(issued_table).toPandas()
        issued = {
            "report": count_flow(issued_cohort, transition_stages(primary_gate=True), unit="windows"),
            "by_group": count_flow(issued_cohort, transition_stages(), unit="windows"),
        }
        print(f"  Issued vintage read from {issued_table}")
    else:
        print(f"  No issued cohort table at {issued_table}; the issued columns stay blank")
    return current, issued


def build_table_6_3a(current_report, issued_report):
    """The report's Table 6.3a: the twelve stages, users and windows, with
    the issued vintage's users beside them and the difference."""
    users = side_by_side(current_report, issued_report, value="n_users", group=ALL_GROUPS_LABEL)
    windows = current_report.loc[current_report["group"] == ALL_GROUPS_LABEL, ["stage", "n_windows"]]
    table = users.merge(windows, on="stage", how="left")
    table = table.rename(columns={"n_users_current": "n_users", "n_windows": "n_segments",
                                  "n_users_issued": "n_users_issued", "difference": "difference_users"})
    table["n_segments"] = table["n_segments"].astype("Int64")
    return table[["stage", "description", "n_users", "n_segments", "n_users_issued", "difference_users"]]


def write_csv(frame, output_dir, name):
    assert "_userId" not in frame.columns, f"{name}: a per-user column must not be written"
    path = os.path.join(output_dir, name)
    frame.to_csv(path, index=False)
    print(f"  Saved: {path}")


def run_analysis(spark, output_dir=None, suffix="", issued_table=None):
    if output_dir is None:
        output_dir = OUTPUT_DIR + suffix
    if issued_table is None:
        issued_table = f"{CATALOG}.{ISSUED_COHORT_TABLE.format(suffix=suffix)}"
    # The analysis owns its output folder (STYLE.md §5).
    shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Table 6.3a and the cohort flows (PLN-1001 populations)")
    print("=" * 60)

    print(f"\n1. Transition cohort flow (suffix={suffix!r})...")
    current, issued = transition_flows(spark, suffix, issued_table)
    for name, long in current.items():
        assert_monotone(long)
    table_6_3a = build_table_6_3a(current["report"], issued["report"] if issued else None)
    write_csv(table_6_3a, output_dir, "table_6_3a_cohort_flow.csv")
    write_csv(widen_flow(current["by_group"]), output_dir, "table_6_3a_cohort_flow_by_diagnosis_group.csv")
    long_both = current["by_group"].assign(vintage="current")
    if issued:
        long_both = pd.concat([long_both, issued["by_group"].assign(vintage="issued")], ignore_index=True)
    write_csv(long_both, output_dir, "table_flow_transition_long.csv")
    write_csv(widen_flow(current["carbs"]), output_dir, "table_flow_carbohydrates.csv")
    print("\n" + table_6_3a.to_string(index=False))

    print("\n2. Preset-activation flow...")
    activations = spark.table(f"{CATALOG}.{ACTIVATION_COHORT_TABLE.format(suffix=suffix)}").toPandas()
    for ab_segment in ("seg2", "seg3"):
        long = count_flow(activations, activation_stages(ab_segment), unit="activations")
        assert_monotone(long)
        wide = widen_flow(long).merge(widen_flow(long, value="n_activations"),
                                      on=["stage", "description"], suffixes=("_users", "_activations"))
        write_csv(wide, output_dir, f"table_flow_preset_activations_{ab_segment}.csv")

    results = {"table_6_3a": table_6_3a, "transition_long": long_both}
    if suffix == "":
        print("\n3. Stable-autobolus flow...")
        stable = spark.table(f"{CATALOG}.{STABLE_COHORT_TABLE}").toPandas()
        for with_guardrail, name in ((False, "table_flow_stable_autobolus.csv"),
                                     (True, "table_flow_stable_autobolus_with_guardrail.csv")):
            long = count_flow(stable, stable_stages(with_guardrail), unit="segments")
            assert_monotone(long)
            write_csv(widen_flow(long), output_dir, name)

        print("\n4. Durability flow, one table per coverage build present...")
        for coverage in COVERAGE_SUFFIXES:
            table = f"{CATALOG}.{DURABILITY_COHORT_TABLE.format(suffix=coverage)}"
            if not _table_exists(spark, table):
                print(f"  {table} absent, skipped")
                continue
            long = count_flow(spark.table(table).toPandas(), durability_stages(), unit="users")
            assert_monotone(long)
            write_csv(widen_flow(long), output_dir, f"table_flow_durability{coverage}.csv")

    print("\n5. Gate C: the final stage equals the loader's cohort...")
    cohort = load_transition_endpoints(spark, suffix=suffix)
    final = int(table_6_3a.loc[table_6_3a["stage"] == "Final transition cohort", "n_users"].iloc[0])
    assert final == len(cohort), (
        f"Table 6.3a's final stage ({final}) differs from the loader's cohort ({len(cohort)})"
    )
    print(f"  Final transition cohort = {final} users on both paths")
    results["df"] = cohort

    print("\n" + "=" * 60)
    print("Table 6.3a Complete!")
    print(f"Outputs saved to: {output_dir}/")
    print("=" * 60)
    return results


def run_in_databricks(spark, suffix=""):
    return run_analysis(spark, suffix=suffix)


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default="",
                         help="source-table suffix, e.g. _box070 for the 10-of-14 cohort")
    _args, _ = _parser.parse_known_args()
    run_in_databricks(spark, suffix=_args.suffix)  # type: ignore[name-defined]
