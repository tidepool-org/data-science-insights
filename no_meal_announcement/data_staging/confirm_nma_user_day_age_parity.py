"""Confirm a Databricks `nma_user_day_age` regeneration matches the local analysis-ready snapshot the
report's age-stratified §8.1 (adult/pediatric) numbers were taken from (developer_note ask #3).

`age_years = DATEDIFF(local_day, dob)/365.25` is computed **as of the measurement day**
(export_user_day_age.py) — deterministic, so a re-run can only differ if an UPSTREAM input changed:
the `loop_recommendations` day universe, the `bddp_user_dates` DOB lookup, or the cutoff constants
(PEDIATRIC_AGE_CUTOFF / MAX_PLAUSIBLE_AGE). This prints the by-`is_pediatric` distribution of the age
columns as they land in the analysis-ready snapshot, at two levels:

  - raw       : the full analysis-ready table (the strongest, most-upstream parity check — if this
                matches, every downstream deterministic transform matches too).
  - eligible  : day_eligible AND user_eligible (= prepare_day_level / §7.1) — the cohort the report's
                §8.1 sample-information + adult/pediatric numbers are drawn from (the §6 age floor and
                the cohort split in filter_cohort are applied further downstream, also deterministic).

Run it in both environments and diff the printed tables — identical ⇒ regeneration confirmed.

The comparison is hash-invariant: `_userId` is pseudonymized (D16) identically in the local CSV and
the Databricks table, and the hash is a bijection on users, so COUNT(DISTINCT _userId) and the day
counts are directly comparable.

Usage
-----
  # 1. LOCAL reference (no Spark) — the snapshot the report used:
  python data_staging/confirm_nma_user_day_age_parity.py

  # 2. On Databricks, after regenerating the age table + re-exporting the analysis-ready table:
  #      %run .../export_user_day_age.py            (regenerate nma_user_day_age)
  #      %run .../export_user_day_analysis_ready.py (re-join age into the analysis-ready table)
  #    then run this file (a `spark` global is present) → it reads the regenerated table.

  # 3. Diff the two printed tables. Any row that differs points at a changed upstream input.
"""

import os

import pandas as pd

LOCAL_CSV = os.path.join(os.path.dirname(__file__), "..", "outputs", "nma_user_day_analysis_ready.csv")
ANALYSIS_READY_TABLE = "dev.fda_510k_rwd.nma_user_day_analysis_ready"

# Expected LOCAL reference (outputs/nma_user_day_analysis_ready.csv, 2026-06-07) — the snapshot the
# report's §8.1 numbers were taken from. A Databricks regen should reproduce these exactly.
# (bucket: pediatric = is_pediatric TRUE, adult = FALSE, unknown = NULL DOB.)
EXPECTED = {
    "raw": {
        "adult":             (1877, 740174, 40.029),
        "pediatric":         (554, 207894, 12.350),
        "unknown(DOB null)": (2, 746, None),
    },
    "eligible": {
        "adult":             (1752, 696563, 40.154),
        "pediatric":         (516, 196169, 12.350),
        "unknown(DOB null)": (2, 743, None),
    },
}


def _print_expected(level):
    print(f"  expected[{level}]: " + "  ".join(
        f"{b}=({u}u/{d}d, mean {a})" for b, (u, d, a) in EXPECTED[level].items()))


def run_local():
    pdf = pd.read_csv(LOCAL_CSV)
    pdf["_bucket"] = pdf["is_pediatric"].map({True: "pediatric", False: "adult"}).fillna("unknown(DOB null)")
    print(f"LOCAL analysis-ready snapshot: {os.path.abspath(LOCAL_CSV)}")
    for level, frame in (("raw", pdf),
                         ("eligible", pdf[(pdf["day_eligible"] == True) & (pdf["user_eligible"] == True)])):  # noqa: E712
        agg = (frame.groupby("_bucket")
                    .agg(n_users=("_userId", "nunique"), n_days=("_userId", "size"),
                         mean_age=("age_years", "mean"))
                    .reset_index())
        agg["mean_age"] = agg["mean_age"].round(3)
        print(f"\n[{level}]")
        print(agg.to_string(index=False))
        _print_expected(level)


def run_databricks(spark):
    print(f"DATABRICKS regenerated table: {ANALYSIS_READY_TABLE}")
    for level, where in (("raw", ""), ("eligible", "WHERE day_eligible AND user_eligible")):
        df = spark.sql(f"""
            SELECT
              CASE WHEN is_pediatric IS NULL THEN 'unknown(DOB null)'
                   WHEN is_pediatric THEN 'pediatric' ELSE 'adult' END AS _bucket,
              COUNT(DISTINCT _userId)  AS n_users,
              COUNT(*)                 AS n_days,
              ROUND(AVG(age_years), 3) AS mean_age
            FROM {ANALYSIS_READY_TABLE}
            {where}
            GROUP BY 1 ORDER BY 1
        """).toPandas()
        print(f"\n[{level}]")
        print(df.to_string(index=False))
        _print_expected(level)
    print("\n⇒ Both levels identical to the LOCAL reference ⇒ regeneration confirmed. Any drift ⇒ a "
          "changed upstream (loop_recommendations day universe, bddp_user_dates DOB, or age cutoff).")


if __name__ == "__main__":
    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821  (Databricks injects this)
    except NameError:
        _spark = None
    (run_databricks(_spark) if _spark is not None else run_local())
