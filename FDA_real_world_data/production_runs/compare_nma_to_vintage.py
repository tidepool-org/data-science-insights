"""Tie a regenerated `nma_user_day_analysis_ready` to the issued vintage's copy, by age bucket.

Until 2026-11-05 this file pinned the counts of the 2026-06-07 snapshot in the repository.
It now reads both tables on Databricks (the regenerated table and the snapshot copy
`nma_user_day_analysis_ready_vintage_2026_10_29`, production_runs/snapshot_vintage.py) and
prints them side by side with the difference, so the comparison is a tie-out against the
vintage (execution plan Step 3 item 4) and no dataset number lives in the repository.

`age_years = DATEDIFF(local_day, dob)/365.25` is computed as of the measurement day
(export_nma_user_day_age.py), so a difference can only come from an upstream change: the day
universe (the FDA day table, rebuilt on the corrected classifier), the DOB lookup, or the
age cutoffs. A difference in the universe is expected after the rebuild; the point of the
table is to see where it lands.

Two levels per table:
  - raw       : every row of the table
  - eligible  : the rows the analyses start from (the current table's in_analysis_cohort;
                the vintage copy's day_eligible AND user_eligible, which was its equivalent
                before the version and age gates became columns)

Usage (Databricks; a `spark` global is present):
  %run .../compare_nma_to_vintage.py
  python data_staging/compare_nma_to_vintage.py --current <table> --vintage <table>
Locally (no Spark) it prints the same buckets for the CSV snapshot only.
"""

import argparse
import os

import pandas as pd

LOCAL_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "outputs", "nma", "nma_user_day_analysis_ready.csv")
ANALYSIS_READY_TABLE = "dev.fda_510k_rwd.nma_user_day_analysis_ready"
VINTAGE_TABLE = "dev.fda_510k_rwd.nma_user_day_analysis_ready_vintage_2026_10_29"

BUCKET_SQL = ("CASE WHEN is_pediatric IS NULL THEN 'unknown(DOB null)' "
              "WHEN is_pediatric THEN 'pediatric' ELSE 'adult' END")


def _eligible_clause(spark, table):
    """The analysis cohort's rows: the cohort predicate where the table carries one, the
    two eligibility flags where it predates the cohort columns."""
    columns = set(spark.table(table).columns)
    if "in_analysis_cohort" in columns:
        return "WHERE in_analysis_cohort"
    return "WHERE day_eligible AND user_eligible"


def buckets(spark, table, level):
    where = "" if level == "raw" else _eligible_clause(spark, table)
    return spark.sql(f"""
        SELECT {BUCKET_SQL} AS bucket,
               COUNT(DISTINCT _userId)  AS n_users,
               COUNT(*)                 AS n_days,
               ROUND(AVG(age_years), 3) AS mean_age
        FROM {table}
        {where}
        GROUP BY 1 ORDER BY 1
    """).toPandas()


def run_databricks(spark, current=ANALYSIS_READY_TABLE, vintage=VINTAGE_TABLE):
    print(f"current: {current}\nvintage: {vintage}")
    for level in ("raw", "eligible"):
        now = buckets(spark, current, level).set_index("bucket")
        then = buckets(spark, vintage, level).set_index("bucket")
        table = now.join(then, how="outer", lsuffix="_current", rsuffix="_vintage")
        for column in ("n_users", "n_days"):
            table[f"{column}_difference"] = table[f"{column}_current"] - table[f"{column}_vintage"]
        print(f"\n[{level}]")
        print(table.reset_index().to_string(index=False))
    print("\nA difference in n_days or n_users is the rebuilt day universe; a difference in mean_age "
          "at equal counts is the DOB lookup or the age cutoffs.")


def run_local():
    pdf = pd.read_csv(LOCAL_CSV)
    pdf["_bucket"] = pdf["is_pediatric"].map({True: "pediatric", False: "adult"}).fillna("unknown(DOB null)")
    eligible = pdf["in_analysis_cohort"].eq(True) if "in_analysis_cohort" in pdf.columns \
        else (pdf["day_eligible"].eq(True) & pdf["user_eligible"].eq(True))
    print(f"LOCAL analysis-ready snapshot: {os.path.abspath(LOCAL_CSV)}")
    for level, frame in (("raw", pdf), ("eligible", pdf[eligible])):
        agg = (frame.groupby("_bucket")
                    .agg(n_users=("_userId", "nunique"), n_days=("_userId", "size"),
                         mean_age=("age_years", "mean"))
                    .reset_index())
        agg["mean_age"] = agg["mean_age"].round(3)
        print(f"\n[{level}]")
        print(agg.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", default=ANALYSIS_READY_TABLE)
    parser.add_argument("--vintage", default=VINTAGE_TABLE)
    args, _ = parser.parse_known_args()
    try:
        _spark = spark  # type: ignore[name-defined]  # noqa: F821  (Databricks injects this)
    except NameError:
        _spark = None
    (run_databricks(_spark, args.current, args.vintage) if _spark is not None else run_local())
