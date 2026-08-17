"""Rank behavior-trace candidate users and persist the result.

The candidate scan (full BDDP food + dosingDecision aggregation) is the slow
half of the trace export, so it runs once here and saves to CANDIDATES_TABLE;
export_behavior_traces.py then reads the table and can be re-run cheaply.

Selection targets users where NO timestamp fallbacks are needed: every carb
entry carries Loop's app-side entry clock (payload key
com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate), the modern-era record is
long, CGM coverage is high, and loop dosing decisions carry insulinOnBoard.

Run on Databricks. The saved table holds raw _userIds and per-user stats --
it stays on Databricks; only the pseudonymized CSVs from the export script
ever leave.
"""

import argparse

BDDP_TABLE = "dev.default.bddp_sample_all_2"
CBG_TABLE = "dev.fda_510k_rwd.loop_cbg"
CANDIDATES_TABLE = "dev.fda_510k_rwd.behavior_trace_candidates"

ENTRY_CLOCK_KEY = "com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate"

# Selection gates (handoff §7-P1: prefer length over cleanliness)
MIN_ENTRY_CLOCK_FRAC = 0.95   # of the user's food rows in the modern window
MIN_SPAN_DAYS = 180           # first->last entry-clock food row
MIN_CARBS_PER_DAY = 1.0
MIN_CGM_COMPLETENESS = 0.70   # plausible readings / (span_days * 288)
MIN_IOB_FRAC = 0.90           # loop dosing decisions carrying insulinOnBoard
MIN_BOLUS_FLAG_FRAC = 0.90    # subType='normal' bolus rows carrying the HK
                              # MetadataKeyAutomaticallyIssued flag -- without
                              # it, manual vs autobolus is unclassifiable on
                              # the HealthKit upload path (first 2-user export)
MAX_CANDIDATES = 50           # pool size saved; export picks from the top

# both bracket-quote styles work in Spark (verified Q9b); single-quote kept
# for consistency with the carb extraction. Values arrive as '1.0'/'0.0'.
AUTO_FLAG_PATH = "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']"


def run(spark, candidates_table=CANDIDATES_TABLE):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {candidates_table} AS
    WITH food AS (
      SELECT
        _userId,
        COUNT(*) AS n_food,
        AVG(CASE WHEN payload LIKE '%{ENTRY_CLOCK_KEY}%' THEN 1.0 ELSE 0.0 END)
          AS frac_entry_clock,
        MIN(CASE WHEN payload LIKE '%{ENTRY_CLOCK_KEY}%'
                 THEN TRY_CAST(time_string AS TIMESTAMP) END) AS first_clock_ts,
        MAX(CASE WHEN payload LIKE '%{ENTRY_CLOCK_KEY}%'
                 THEN TRY_CAST(time_string AS TIMESTAMP) END) AS last_clock_ts
      FROM {BDDP_TABLE}
      WHERE type = 'food'
        AND nutrition IS NOT NULL
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
      GROUP BY _userId
    ),
    windowed AS (
      SELECT *,
        DATEDIFF(last_clock_ts, first_clock_ts) AS span_days
      FROM food
      WHERE first_clock_ts IS NOT NULL
    ),
    dd AS (
      SELECT
        _userId,
        COUNT(*) AS n_dd,
        AVG(CASE WHEN insulinOnBoard IS NOT NULL AND insulinOnBoard != ''
                 THEN 1.0 ELSE 0.0 END) AS frac_iob,
        AVG(CASE WHEN recommendedBolus IS NOT NULL AND recommendedBolus != ''
                 THEN 1.0 ELSE 0.0 END) AS frac_rec
      FROM {BDDP_TABLE}
      WHERE type = 'dosingDecision' AND reason = 'loop'
      GROUP BY _userId
    ),
    cgm AS (
      SELECT c._userId, COUNT(*) AS n_cbg
      FROM {CBG_TABLE} c
      INNER JOIN windowed w
        ON c._userId = w._userId
       AND c.cbg_timestamp >= w.first_clock_ts
       AND c.cbg_timestamp <= w.last_clock_ts
      WHERE c.is_plausible
      GROUP BY c._userId
    ),
    -- Manual-vs-autobolus classifiability: the HK automatically-issued flag
    -- must ride on the user's bolus rows themselves. Same JSON path as the
    -- FDA Method-2 classifier (export_loop_recommendations.py); users whose
    -- uploads lack it (e.g. HealthKit path without Loop metadata) are
    -- unclassifiable per-bolus and excluded up front.
    bolus_flag AS (
      SELECT
        b._userId,
        COUNT(*) AS n_bolus,
        AVG(CASE WHEN get_json_object(b.payload, "{AUTO_FLAG_PATH}") IS NOT NULL
                 THEN 1.0 ELSE 0.0 END) AS frac_bolus_flag
      FROM {BDDP_TABLE} b
      INNER JOIN windowed w
        ON b._userId = w._userId
       AND TRY_CAST(b.time_string AS TIMESTAMP) >= w.first_clock_ts
       AND TRY_CAST(b.time_string AS TIMESTAMP) <= w.last_clock_ts
      WHERE b.type = 'bolus'
        AND b.subType = 'normal'
      GROUP BY b._userId
    )
    SELECT
      w._userId,
      w.span_days,
      w.n_food,
      ROUND(w.n_food / GREATEST(w.span_days, 1), 2) AS carbs_per_day,
      ROUND(w.frac_entry_clock, 3) AS frac_entry_clock,
      ROUND(g.n_cbg / (GREATEST(w.span_days, 1) * 288.0), 3) AS cgm_completeness,
      ROUND(d.frac_iob, 3) AS frac_iob,
      ROUND(d.frac_rec, 3) AS frac_rec_on_loop_dd,
      ROUND(f.frac_bolus_flag, 3) AS frac_bolus_flag,
      f.n_bolus,
      w.first_clock_ts,
      w.last_clock_ts,
      CURRENT_TIMESTAMP() AS computed_at
    FROM windowed w
    INNER JOIN dd d ON w._userId = d._userId
    INNER JOIN cgm g ON w._userId = g._userId
    INNER JOIN bolus_flag f ON w._userId = f._userId
    WHERE w.frac_entry_clock >= {MIN_ENTRY_CLOCK_FRAC}
      AND w.span_days >= {MIN_SPAN_DAYS}
      AND w.n_food / GREATEST(w.span_days, 1) >= {MIN_CARBS_PER_DAY}
      AND g.n_cbg / (GREATEST(w.span_days, 1) * 288.0) >= {MIN_CGM_COMPLETENESS}
      AND d.frac_iob >= {MIN_IOB_FRAC}
      AND f.frac_bolus_flag >= {MIN_BOLUS_FLAG_FRAC}
    ORDER BY w.span_days DESC
    LIMIT {MAX_CANDIDATES}
    ;
    """)

    saved = spark.sql(
        f"SELECT * FROM {candidates_table} ORDER BY span_days DESC"
    ).toPandas()
    print(f"Saved {len(saved)} candidates to {candidates_table}. Top 15:")
    print(saved.head(15).to_string(index=False))
    if saved.empty:
        print("No users passed the gates -- loosen MIN_SPAN_DAYS first.")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--candidates_table", default=CANDIDATES_TABLE)
    _args, _ = _parser.parse_known_args()

    run(spark, candidates_table=_args.candidates_table)
