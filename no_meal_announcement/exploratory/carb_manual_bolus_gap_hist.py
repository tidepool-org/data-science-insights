"""Histogram: time from each CARB entry to its CLOSEST manual bolus — ANALYSIS population.

The inverse of `manual_bolus_carb_gap_hist.py` (which anchors on each MANUAL bolus → nearest carb).
Here we anchor on each CARB entry → nearest MANUAL bolus, so the population is carb entries, not
boluses. Same event tables, same eligibility scope, same sign convention — directly comparable.

Fully off STAGED EVENT-LEVEL tables — no raw BDDP. Generate both event tables once first:
  data_staging/export_carb_events.run(spark)           ->  nma_carb_events
  data_staging/export_bolus_classification.run(spark)  ->  nma_bolus_classification
Carb times come from `nma_carb_events` (one row per logical carb entry, carb_ts); manual boluses
from `nma_bolus_classification` (one row per logical bolus, bolus_ts + classification — so the HK+dd
split is NOT recomputed). (The day-grain nma_user_day_* tables can't be used — they carry counts,
not timestamps.)

Scope: ELIGIBLE users on ELIGIBLE days (the analysis population) via nma_user_day_classification
(raw _userId, joins to both event tables).

'MANUAL' = classification='manual' AND is_normal=1 from the table (production rule). The small
HK-silent cadence leak it still contains is random w.r.t. carbs, so it only adds a low flat
background; the meal spike is unaffected.

Gap = signed minutes (nearest_bolus_ts - carb_ts): + = a manual bolus AFTER the carb (the
meal-announce-then-bolus shape), - = the nearest manual bolus is BEFORE the carb entry. Sign matches
the bolus-anchored script (+ = bolus after carb). Nearest manual bolus via window functions over the
merged per-user timeline. Carbs whose user has NO manual bolus on the whole timeline have a NULL gap
and drop out (reported as a separate count).

Prints the binned table (paste-able) + writes a PNG. Run on Databricks: `run(spark)`.
"""
import argparse
import os

CARB_EVENTS = "dev.fda_510k_rwd.nma_carb_events"                 # event-level carbs (generate first)
BOLUS_CLS = "dev.fda_510k_rwd.nma_bolus_classification"          # event-level boluses (generate first)
CLASSIFICATION = "dev.fda_510k_rwd.nma_user_day_classification"  # eligibility (raw _userId)
BIN_MIN = 1
RANGE_MIN = 180
OUT_PNG = "carb_manual_bolus_gap_hist.png"


def _gaps_view_sql(carb_events, bolus_cls, classification):
    return f"""
WITH eligible_days AS (   -- analysis population: eligible users x eligible days (raw _userId)
  SELECT _userId, local_day
  FROM {classification}
  WHERE user_eligible = true AND day_eligible = true
),
carbs AS (                -- from the generated event-level carb table
  SELECT c._userId, c.carb_ts AS ts
  FROM {carb_events} c
  INNER JOIN eligible_days e ON c._userId = e._userId AND c.local_day = e.local_day
),
manual_boluses AS (       -- from the generated event-level classification; no raw / no dd recompute
  SELECT bc._userId, bc.bolus_ts AS ts
  FROM {bolus_cls} bc
  INNER JOIN eligible_days e ON bc._userId = e._userId AND bc.local_day = e.local_day
  WHERE bc.classification = 'manual' AND bc.is_normal = 1
),
events AS (   -- merged per-user timeline: carbs (bolus_ts NULL) + manual boluses (bolus_ts = ts)
  SELECT _userId, ts, 0 AS is_bolus, CAST(NULL AS TIMESTAMP) AS bolus_ts FROM carbs
  UNION ALL
  SELECT _userId, ts, 1 AS is_bolus, ts AS bolus_ts FROM manual_boluses
),
neighbors AS (
  SELECT _userId, ts, is_bolus,
    MAX(bolus_ts) OVER (PARTITION BY _userId ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_bolus,
    MIN(bolus_ts) OVER (PARTITION BY _userId ORDER BY ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_bolus
  FROM events
),
gaps AS (
  SELECT
    CASE
      WHEN prev_bolus IS NULL AND next_bolus IS NULL THEN NULL
      WHEN prev_bolus IS NULL THEN  (unix_timestamp(next_bolus) - unix_timestamp(ts)) / 60.0   -- only a later bolus: + (after)
      WHEN next_bolus IS NULL THEN  (unix_timestamp(prev_bolus) - unix_timestamp(ts)) / 60.0   -- only an earlier bolus: - (before)
      WHEN (unix_timestamp(ts) - unix_timestamp(prev_bolus)) <= (unix_timestamp(next_bolus) - unix_timestamp(ts))
        THEN  (unix_timestamp(prev_bolus) - unix_timestamp(ts)) / 60.0   -- nearer the prior bolus (bolus BEFORE the carb) -> -
      ELSE   (unix_timestamp(next_bolus) - unix_timestamp(ts)) / 60.0    -- nearer the next bolus (bolus AFTER the carb)  -> +
    END AS gap_min
  FROM neighbors WHERE is_bolus = 0
)
SELECT gap_min FROM gaps
"""


def run(spark, carb_events=CARB_EVENTS, bolus_cls=BOLUS_CLS, classification=CLASSIFICATION):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW _carb_bolus_gaps AS "
              f"{_gaps_view_sql(carb_events, bolus_cls, classification)}")

    binned = spark.sql(f"""
      SELECT CAST(FLOOR(gap_min / {BIN_MIN}) AS INT) * {BIN_MIN} AS bin_start_min, COUNT(*) AS n
      FROM _carb_bolus_gaps
      WHERE gap_min BETWEEN -{RANGE_MIN} AND {RANGE_MIN}
      GROUP BY 1 ORDER BY 1
    """).toPandas()

    summ = spark.sql(f"""
      SELECT COUNT(*) AS n_carb_entries,
        SUM(CASE WHEN gap_min IS NULL THEN 1 ELSE 0 END) AS no_manual_bolus_ever,
        SUM(CASE WHEN ABS(gap_min) <= 5   THEN 1 ELSE 0 END) AS within_5min,
        SUM(CASE WHEN ABS(gap_min) <= 15  THEN 1 ELSE 0 END) AS within_15min,
        SUM(CASE WHEN gap_min BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS bolus_after_0_5min,
        SUM(CASE WHEN ABS(gap_min) > {RANGE_MIN} THEN 1 ELSE 0 END) AS beyond_range,
        ROUND(PERCENTILE(ABS(gap_min), 0.5), 1) AS median_abs_gap_min
      FROM _carb_bolus_gaps
    """).toPandas().iloc[0]

    n = int(summ["n_carb_entries"])
    n_gap = n - int(summ["no_manual_bolus_ever"])  # carbs with a finite nearest-bolus gap
    print(f"carb entries (nma_carb_events, eligible users+days): {n:,}")
    print(f"  with a manual bolus on the timeline: {n_gap:,}   "
          f"(no manual bolus ever: {summ['no_manual_bolus_ever']/n:.1%})")
    print(f"  within +/-5 min of a manual bolus:  {summ['within_5min']/n:.1%}   "
          f"(0..+5 min, bolus AFTER the carb: {summ['bolus_after_0_5min']/n:.1%})")
    print(f"  within +/-15 min:                   {summ['within_15min']/n:.1%}")
    print(f"  median |gap| (carbs w/ a bolus):    {summ['median_abs_gap_min']} min")
    print(f"  nearest bolus beyond +/-{RANGE_MIN} min:    {summ['beyond_range']/n:.1%}")
    print("\nbin_start_min, n  (paste-able):")
    print(binned.to_string(index=False))

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(1, 2, figsize=(13, 4.2))
        for a, lo, hi, title in [(ax[0], -RANGE_MIN, RANGE_MIN, f"full +/-{RANGE_MIN} min"),
                                 (ax[1], -20, 20, "zoom +/-20 min")]:
            sub = binned[(binned.bin_start_min >= lo) & (binned.bin_start_min < hi)]
            a.bar(sub.bin_start_min, sub.n, width=BIN_MIN, align="edge", color="#1f77b4")
            a.axvline(0, color="k", lw=0.8, ls="--")
            a.set_title(title)
            a.set_xlabel("minutes (nearest manual bolus − carb;  + = bolus after carb)")
            a.set_ylabel("carb entries")
        fig.suptitle("Time from each carb entry to its nearest manual bolus (eligible users + days)")
        fig.tight_layout()
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else ".", OUT_PNG)
        fig.savefig(out, dpi=130)
        print(f"\nwrote {out}")
    except Exception as e:  # noqa: BLE001
        print(f"\n(plot skipped: {e}; use the binned table above)")

    return binned


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    p = argparse.ArgumentParser()
    p.add_argument("--carb_events", default=CARB_EVENTS)
    p.add_argument("--bolus_cls", default=BOLUS_CLS)
    p.add_argument("--classification", default=CLASSIFICATION)
    a, _ = p.parse_known_args()
    run(spark, a.carb_events, a.bolus_cls, a.classification)
