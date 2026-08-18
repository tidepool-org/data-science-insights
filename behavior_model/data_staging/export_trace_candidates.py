"""Rank cohort-B behavior-trace candidates (dense-IOB + entry-clock) and
persist the result; plot IOB/CGM coverage for the selected sample.

Cohort B (2026-08-18, from the Q10-Q12 findings): `dosingDecision` rows are
uploaded only by the direct Loop->Tidepool uploader, and hundreds of DIY
Loop 3.x users ran it continuously for months-to-years. Their food arrives
through BOTH channels -- HealthKit-path rows carry the app-side entry clock
(payload key com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate), direct-path
rows never do -- and on days when HealthKit sync is active the direct rows
are near-fully duplicated by clocked HK rows (Q12a: duplicate fraction
tracks the clocked-day fraction). So the selection window per user is the
INTERSECTION of their longest full-cadence DD run with their clocked-food
period, and all gates apply inside it: per-tick IOB and entry-clocked carbs
coexist there by construction. Selection from the eligible pool is a
seeded, deterministic random order (`selection_rank` = sha2 of the raw id
+ SAMPLE_SALT), not span-ranked -- cohort A's span ranking selected for
record length; cohort B samples the eligible population.

Cohort A's table (dev.fda_510k_rwd.behavior_trace_candidates) is NOT
touched: its persisted ranking defines the existing 20-user cohort and the
train/dev parity split, and must stay frozen. This script writes the B
table; export_behavior_traces.py reads it and exports the sample.

The candidate scan (full BDDP food + dosingDecision aggregation) is the
slow half of the trace export, so it runs once here; the export script can
then be re-run cheaply.

Run on Databricks. The saved table holds raw _userIds and per-user stats --
it stays on Databricks; only the pseudonymized CSVs (and the coverage plot,
which uses pseudonymized ids) ever leave.
"""

import argparse
import os

BDDP_TABLE = "dev.default.bddp_sample_all_2"
CBG_TABLE = "dev.fda_510k_rwd.loop_cbg"
CANDIDATES_TABLE = "dev.fda_510k_rwd.behavior_trace_candidates_b"

ENTRY_CLOCK_KEY = "com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate"
HK_SOURCE_PATH = "$.payload.sourceRevision.source.name"

# Selection gates, all applied INSIDE the intersection window
DD_ACTIVE_MIN_PER_DAY = 100   # loop DDs/day for a "full-cadence" day (288 =
                              # perfect 5-min cycle; >=100 = uploader ran most
                              # of the day, excludes trickle/backfill days)
MIN_WINDOW_DAYS = 180         # DD-run ∩ clocked-food intersection length
MIN_CLOCK_DAY_FRAC = 0.70     # window days with >=1 clocked carb entry --
                              # guards against long HK-sync-off gaps inside
                              # the window (Q12a: good users sit 0.7-1.0)
MIN_CARBS_PER_DAY = 1.0       # clocked entries per window day
MIN_CGM_COMPLETENESS = 0.70   # plausible readings / (window_days * 288);
                              # note loop_cbg is FDA-staged -- a user absent
                              # from it fails this gate even if raw cbg rows
                              # exist (acceptable shrinkage; revisit if the
                              # pool comes up short)
MIN_IOB_FRAC = 0.90           # in-window loop DDs carrying insulinOnBoard
MIN_BOLUS_FLAG_FRAC = 0.90    # in-window subType='normal' HK bolus rows
                              # carrying the auto-issued flag (manual-vs-auto
                              # classifiability; the DD fallback also works
                              # here since in-window DDs are dense)

N_SELECT_USERS = 10           # the exported sample: lowest selection_rank
SAMPLE_SALT = "cohort-b-sample-v1"  # seeded deterministic random order --
                                    # changing it draws a different sample

# both bracket-quote styles work in Spark (verified Q9b); single-quote kept
# for consistency with the carb extraction. Values arrive as '1.0'/'0.0'.
AUTO_FLAG_PATH = "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']"

PLOT_DIR = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
    "/behavior_model/data_staging/exports_b"
)
USERID_SALT = "behavior-model-v1"  # must match export_behavior_traces.py so
                                   # plot labels align with the exported ids


def run(spark, candidates_table=CANDIDATES_TABLE):
    spark.sql(f"""
    --begin-sql
    CREATE OR REPLACE TABLE {candidates_table} AS
    -- longest contiguous full-cadence DD run per user (gaps-and-islands)
    WITH dd_daily AS (
      SELECT
        _userId,
        DATE(TRY_CAST(time_string AS TIMESTAMP)) AS d,
        COUNT(*) AS n_dd,
        SUM(CASE WHEN insulinOnBoard IS NOT NULL AND insulinOnBoard != ''
                 THEN 1 ELSE 0 END) AS n_dd_iob,
        SUM(CASE WHEN recommendedBolus IS NOT NULL AND recommendedBolus != ''
                 THEN 1 ELSE 0 END) AS n_dd_rec
      FROM {BDDP_TABLE}
      WHERE type = 'dosingDecision' AND reason = 'loop'
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
      GROUP BY 1, 2
    ),
    islands AS (
      SELECT _userId, d,
        DATE_SUB(d, CAST(ROW_NUMBER() OVER (
          PARTITION BY _userId ORDER BY d) AS INT)) AS grp
      FROM dd_daily
      WHERE n_dd >= {DD_ACTIVE_MIN_PER_DAY}
    ),
    runs AS (
      SELECT _userId, grp, COUNT(*) AS run_days,
             MIN(d) AS run_start, MAX(d) AS run_end
      FROM islands
      GROUP BY 1, 2
    ),
    best_run AS (
      SELECT
        _userId,
        MAX(run_days) AS longest_run_days,
        MAX_BY(run_start, run_days) AS run_start,
        MAX_BY(run_end, run_days) AS run_end
      FROM runs
      GROUP BY 1
    ),
    -- clocked (HK-path, entry-clock-bearing) food days inside the run
    clock_daily AS (
      SELECT
        f._userId,
        DATE(TRY_CAST(f.time_string AS TIMESTAMP)) AS d,
        COUNT(*) AS n_clock_food
      FROM {BDDP_TABLE} f
      INNER JOIN best_run r
        ON f._userId = r._userId
       AND DATE(TRY_CAST(f.time_string AS TIMESTAMP))
           BETWEEN r.run_start AND r.run_end
      WHERE f.type = 'food'
        AND f.nutrition IS NOT NULL
        AND TRY_CAST(f.time_string AS TIMESTAMP) IS NOT NULL
        AND f.payload LIKE '%{ENTRY_CLOCK_KEY}%'
      GROUP BY 1, 2
    ),
    -- intersection window: the run clipped to the clocked-food period.
    -- clock_daily is already restricted to [run_start, run_end], so the
    -- window bounds are simply the first/last clocked day and every
    -- clock_daily row lies inside them
    win AS (
      SELECT
        r._userId,
        r.longest_run_days,
        r.run_start,
        r.run_end,
        MIN(c.d) AS win_start,
        MAX(c.d) AS win_end,
        DATEDIFF(MAX(c.d), MIN(c.d)) + 1 AS window_days,
        COUNT(*) AS clock_days,
        SUM(c.n_clock_food) AS n_clock_food
      FROM best_run r
      INNER JOIN clock_daily c ON r._userId = c._userId
      GROUP BY r._userId, r.longest_run_days, r.run_start, r.run_end
    ),
    dd_win AS (
      SELECT d._userId,
        SUM(d.n_dd) AS n_loop_dd,
        SUM(d.n_dd_iob) / GREATEST(SUM(d.n_dd), 1) AS frac_iob,
        SUM(d.n_dd_rec) / GREATEST(SUM(d.n_dd), 1) AS frac_rec
      FROM dd_daily d
      INNER JOIN win w
        ON d._userId = w._userId AND d.d BETWEEN w.win_start AND w.win_end
      GROUP BY 1
    ),
    cgm AS (
      SELECT c._userId, COUNT(*) AS n_cbg
      FROM {CBG_TABLE} c
      INNER JOIN win w
        ON c._userId = w._userId
       AND c.cbg_timestamp >= CAST(w.win_start AS TIMESTAMP)
       AND c.cbg_timestamp < CAST(DATE_ADD(w.win_end, 1) AS TIMESTAMP)
      WHERE c.is_plausible
      GROUP BY c._userId
    ),
    bolus_flag AS (
      SELECT
        b._userId,
        COUNT(*) AS n_bolus,
        AVG(CASE WHEN get_json_object(b.payload, "{AUTO_FLAG_PATH}") IS NOT NULL
                 THEN 1.0 ELSE 0.0 END) AS frac_bolus_flag
      FROM {BDDP_TABLE} b
      INNER JOIN win w
        ON b._userId = w._userId
       AND TRY_CAST(b.time_string AS TIMESTAMP) >= CAST(w.win_start AS TIMESTAMP)
       AND TRY_CAST(b.time_string AS TIMESTAMP) < CAST(DATE_ADD(w.win_end, 1) AS TIMESTAMP)
      WHERE b.type = 'bolus'
        AND b.subType = 'normal'
        AND get_json_object(b.origin, '{HK_SOURCE_PATH}') = 'Loop'
      GROUP BY b._userId
    )
    SELECT
      w._userId,
      w.window_days AS span_days,
      w.longest_run_days,
      w.run_start,
      w.run_end,
      w.n_clock_food AS n_food,
      ROUND(w.n_clock_food / GREATEST(w.window_days, 1), 2) AS carbs_per_day,
      ROUND(w.clock_days / GREATEST(w.window_days, 1), 3) AS clock_day_frac,
      ROUND(g.n_cbg / (GREATEST(w.window_days, 1) * 288.0), 3) AS cgm_completeness,
      ROUND(d.frac_iob, 3) AS frac_iob,
      ROUND(d.frac_rec, 3) AS frac_rec_on_loop_dd,
      d.n_loop_dd,
      ROUND(f.frac_bolus_flag, 3) AS frac_bolus_flag,
      f.n_bolus,
      -- export window bounds; column names kept from cohort A so
      -- export_behavior_traces joins work unchanged
      CAST(w.win_start AS TIMESTAMP) AS first_clock_ts,
      TIMESTAMPADD(SECOND, -1, CAST(DATE_ADD(w.win_end, 1) AS TIMESTAMP))
        AS last_clock_ts,
      -- deterministic random order: the exported sample is the lowest
      -- N_SELECT_USERS ranks; changing SAMPLE_SALT redraws the sample
      sha2(concat(w._userId, '{SAMPLE_SALT}'), 256) AS selection_rank,
      CURRENT_TIMESTAMP() AS computed_at
    FROM win w
    INNER JOIN dd_win d ON w._userId = d._userId
    INNER JOIN cgm g ON w._userId = g._userId
    INNER JOIN bolus_flag f ON w._userId = f._userId
    WHERE w.window_days >= {MIN_WINDOW_DAYS}
      AND w.clock_days / GREATEST(w.window_days, 1) >= {MIN_CLOCK_DAY_FRAC}
      AND w.n_clock_food / GREATEST(w.window_days, 1) >= {MIN_CARBS_PER_DAY}
      AND g.n_cbg / (GREATEST(w.window_days, 1) * 288.0) >= {MIN_CGM_COMPLETENESS}
      AND d.frac_iob >= {MIN_IOB_FRAC}
      AND f.frac_bolus_flag >= {MIN_BOLUS_FLAG_FRAC}
    ;
    """)

    saved = spark.sql(
        f"SELECT * FROM {candidates_table} ORDER BY selection_rank"
    ).toPandas()
    print(f"Saved {len(saved)} eligible cohort-B candidates to "
          f"{candidates_table}; the export takes the first {N_SELECT_USERS} "
          "by selection_rank (deterministic random). Selected sample:")
    print(saved.head(N_SELECT_USERS)
          .drop(columns=["selection_rank"]).to_string(index=False))
    if saved.empty:
        print("No users passed the gates -- check MIN_CLOCK_DAY_FRAC first "
              "(the loop_cbg staging-scope caveat is the other usual suspect).")
    return saved


def plot_coverage(spark, candidates_table=CANDIDATES_TABLE, plot_dir=PLOT_DIR,
                  n_users=N_SELECT_USERS):
    """One panel per selected user: daily IOB coverage (loop DDs / 288, gray
    fill) and daily CGM completeness (plausible cbg / 288, blue line) across
    the selection window -- the eyeball check that both streams are real
    before committing to the export. Pseudonymized ids only."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    daily = spark.sql(f"""
    --begin-sql
    WITH picked AS (
      SELECT _userId, first_clock_ts, last_clock_ts, selection_rank
      FROM {candidates_table}
      ORDER BY selection_rank
      LIMIT {n_users}
    ),
    dd AS (
      SELECT b._userId, DATE(TRY_CAST(b.time_string AS TIMESTAMP)) AS d,
             COUNT(*) AS n_dd
      FROM {BDDP_TABLE} b
      INNER JOIN picked p
        ON b._userId = p._userId
       AND TRY_CAST(b.time_string AS TIMESTAMP)
           BETWEEN p.first_clock_ts AND p.last_clock_ts
      WHERE b.type = 'dosingDecision' AND b.reason = 'loop'
      GROUP BY 1, 2
    ),
    cbg AS (
      SELECT c._userId, DATE(c.cbg_timestamp) AS d, COUNT(*) AS n_cbg
      FROM {CBG_TABLE} c
      INNER JOIN picked p
        ON c._userId = p._userId
       AND c.cbg_timestamp BETWEEN p.first_clock_ts AND p.last_clock_ts
      WHERE c.is_plausible
      GROUP BY 1, 2
    ),
    merged AS (
      SELECT
        COALESCE(dd._userId, cbg._userId) AS _userId,
        COALESCE(dd.d, cbg.d) AS d,
        COALESCE(dd.n_dd, 0) AS n_dd,
        COALESCE(cbg.n_cbg, 0) AS n_cbg
      FROM dd
      FULL OUTER JOIN cbg
        ON cbg._userId = dd._userId AND cbg.d = dd.d
    )
    SELECT
      concat('u', substr(sha2(concat(p._userId, '{USERID_SALT}'), 256), 1, 16))
        AS uid,
      m.d,
      m.n_dd,
      m.n_cbg
    FROM merged m
    INNER JOIN picked p ON m._userId = p._userId
    ORDER BY p.selection_rank, m.d
    ;
    """).toPandas()

    uids = list(dict.fromkeys(daily["uid"]))
    fig, axes = plt.subplots(len(uids), 1, figsize=(10, 1.4 * len(uids)),
                             sharex=False, squeeze=False)
    for ax, uid in zip(axes[:, 0], uids):
        u = daily[daily["uid"] == uid]
        ax.fill_between(u["d"], (u["n_dd"] / 288.0).clip(upper=1.0),
                        color="#c9c8c1", label="IOB (loop DDs)")
        ax.plot(u["d"], (u["n_cbg"] / 288.0).clip(upper=1.0),
                color="#2a78d6", linewidth=0.9, label="CGM")
        ax.set_ylim(0, 1.05)
        ax.set_ylabel(uid[:9], fontsize=7, rotation=0, ha="right", va="center")
        ax.tick_params(labelsize=7)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
    axes[0, 0].legend(loc="lower left", fontsize=7, frameon=False, ncol=2)
    fig.suptitle("Cohort-B selected sample: daily IOB (gray) and CGM (blue) "
                 "tick coverage over the export window", fontsize=10, x=0.01,
                 ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    os.makedirs(plot_dir, exist_ok=True)
    path = os.path.join(plot_dir, "candidate_coverage.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"Wrote coverage figure to {path}")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--candidates_table", default=CANDIDATES_TABLE)
    _parser.add_argument("--skip_plot", action="store_true")
    _args, _ = _parser.parse_known_args()

    run(spark, candidates_table=_args.candidates_table)
    if not _args.skip_plot:
        plot_coverage(spark, candidates_table=_args.candidates_table)
