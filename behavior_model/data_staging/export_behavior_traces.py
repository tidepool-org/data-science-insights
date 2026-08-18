"""Export full behavior-model traces for the cohort-B sample.

Cohort B (2026-08-18): users whose longest full-cadence dosingDecision run
intersects their entry-clocked (HealthKit-path) food period for >=180 days
-- per-tick IOB and entry-clocked carbs coexist in the export window by
construction. The sample is the N_EXPORT_USERS lowest `selection_rank`
rows (seeded deterministic random order), NOT span-ranked. Carbs are taken
from entry-clock-bearing rows ONLY: on overlap days the direct-path food
rows are near-complete duplicates of the clocked HK rows (Q12a), so this
single filter both dedupes the two upload channels and guarantees the
no-fallback entry-clock property. Boluses keep both channels -- the
existing (user, time, units) dedup merges dual-path copies and the flag /
DD fallback classifies them.

Two-step flow: run export_trace_candidates.py FIRST (the slow full-BDDP
gating scan; persists to CANDIDATES_TABLE), then this script, which reads
the saved candidates and does the comparatively cheap stream export -- so
export tweaks don't pay the gating cost again. Download the CSVs to
behavior_model/data/behavior_traces_b/ locally (keep cohort A's
data/behavior_traces/ intact -- the recorded it00-it05 iterations reproduce
from it) and run build_tick_frame.py --data-dir against it.

Run on Databricks. Emits five CSVs to OUTPUT_DIR, one row-stream each,
with `_userId` pseudonymized (salted SHA-256, NMA convention) so raw ids
never leave Databricks:

  users.csv   : _userId, tz_offset_min, window_start, window_end
  cgm.csv     : _userId, cbg_timestamp (user-local), cbg_mg_dl
  carbs.csv   : _userId, meal_time, entry_time (both user-local), carb_grams,
                absorption_minutes, meal_time_edited
  boluses.csv : _userId, bolus_timestamp (user-local), bolus_units
                (user-initiated only -- autoboluses are the controller's
                actions, not behavior, and would corrupt correction labels)
  dosing.csv  : _userId, dd_timestamp (user-local), reason,
                insulin_on_board_raw, recommended_bolus_raw
                (raw strings -- value shape is parsed locally)

TZ semantics follow simulation/export/export_single_user_day.py: one
per-user offset (latest non-NULL timezoneOffset), because per-row offsets
are inconsistently populated. Sub-record TZ/DST changes are not modelled --
the local weekly drift check will surface a mid-record clock shift.

Per-user export window = the candidates table's [first_clock_ts,
last_clock_ts] -- for cohort B that is the DD-run ∩ clocked-food
intersection window, not the whole record.
"""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor

BDDP_TABLE = "dev.default.bddp_sample_all_2"
CBG_TABLE = "dev.fda_510k_rwd.loop_cbg"
CANDIDATES_TABLE = "dev.fda_510k_rwd.behavior_trace_candidates_b"
OUTPUT_DIR = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
    "/behavior_model/data_staging/exports_b"
)

USERID_SALT = "behavior-model-v1"  # unchanged from cohort A: same raw id
                                   # always pseudonymizes to the same uid
ENTRY_CLOCK_KEY = "com.loopkit.CarbKit.HKMetadataKey.UserCreatedDate"

N_EXPORT_USERS = 10           # cohort B: random sample of the eligible pool
                              # (lowest selection_rank)
OVERRIDE_USER_IDS = []        # raw _userIds; set to skip the sampled pick

def _hash_expr(alias):
    """Pseudonymized _userId (NMA convention), qualified to one join alias so
    multi-table queries don't hit an ambiguous-reference error."""
    return (f"concat('u', substr(sha2(concat({alias}._userId, "
            f"'{USERID_SALT}'), 256), 1, 16))")


def run(spark, output_dir=OUTPUT_DIR, n_users=N_EXPORT_USERS):
    try:
        candidates = spark.sql(
            f"SELECT * FROM {CANDIDATES_TABLE} ORDER BY selection_rank"
        ).toPandas()
    except Exception as exc:
        raise RuntimeError(
            f"could not read {CANDIDATES_TABLE} -- run "
            "export_trace_candidates.py first"
        ) from exc
    print(f"{len(candidates)} candidate(s) in {CANDIDATES_TABLE} "
          "(raw ids stay on Databricks; CSVs are pseudonymized):")
    print(candidates.to_string(index=False))

    if OVERRIDE_USER_IDS:
        picked = candidates[candidates["_userId"].isin(OVERRIDE_USER_IDS)]
    else:
        picked = candidates.head(n_users)
    if picked.empty:
        raise RuntimeError(
            "no users to export -- candidates table is empty (loosen the gates "
            "in export_trace_candidates.py and re-run it) or OVERRIDE_USER_IDS "
            "matched nothing")
    print(f"\nExporting {len(picked)} user(s).")

    spark.createDataFrame(
        picked[["_userId", "first_clock_ts", "last_clock_ts"]]
    ).createOrReplaceTempView("_bm_picked")

    # One TZ offset per user: latest non-NULL over the whole record.
    tz_sql = f"""
    --begin-sql
    SELECT
      b._userId,
      CAST(MAX_BY(b.timezoneOffset, TRY_CAST(b.time_string AS TIMESTAMP)) AS INT)
        AS tz_offset_min
    FROM {BDDP_TABLE} b
    INNER JOIN _bm_picked p ON b._userId = p._userId
    WHERE b.timezoneOffset IS NOT NULL
      AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    GROUP BY b._userId
    ;
    """
    tz_pdf = spark.sql(tz_sql).toPandas()
    spark.createDataFrame(tz_pdf).createOrReplaceTempView("_bm_tz")

    users_sql = f"""
    --begin-sql
    SELECT
      {_hash_expr('p')} AS _userId,
      u.tz_offset_min,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, p.first_clock_ts) AS window_start,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, p.last_clock_ts) AS window_end
    FROM _bm_picked p
    INNER JOIN _bm_tz u ON p._userId = u._userId
    ;
    """

    cgm_sql = f"""
    --begin-sql
    SELECT
      {_hash_expr('c')} AS _userId,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, c.cbg_timestamp) AS cbg_timestamp,
      c.cbg_mg_dl
    FROM {CBG_TABLE} c
    INNER JOIN _bm_picked p
      ON c._userId = p._userId
     AND c.cbg_timestamp >= p.first_clock_ts
     AND c.cbg_timestamp <= p.last_clock_ts
    INNER JOIN _bm_tz u ON c._userId = u._userId
    WHERE c.is_plausible
    ORDER BY 1, 2
    ;
    """

    # Carbs: dedup exact (user, time, grams) keeping latest ingest copy (NMA
    # convention), then shift both clocks to user-local. meal_time_edited:
    # whole-second time_string = user-edited meal time; fractional = untouched
    # "now" default (empirical tell from the P0 payload sampling).
    carbs_sql = f"""
    --begin-sql
    WITH deduped AS (
      SELECT
        _userId,
        TRY_CAST(time_string AS TIMESTAMP) AS meal_utc,
        TRY_CAST(get_json_object(payload, "$['{ENTRY_CLOCK_KEY}']") AS TIMESTAMP)
          AS entry_utc,
        TRY_CAST(get_json_object(nutrition, '$.carbohydrate.net') AS DOUBLE)
          AS carb_grams,
        TRY_CAST(get_json_object(nutrition, '$.estimatedAbsorptionDuration') AS DOUBLE) / 60.0
          AS absorption_minutes,
        time_string NOT LIKE '%.%' AS meal_time_edited,
        ROW_NUMBER() OVER (
          PARTITION BY _userId, time_string,
                       get_json_object(nutrition, '$.carbohydrate.net')
          ORDER BY created_timestamp DESC
        ) AS rn
      FROM {BDDP_TABLE}
      WHERE type = 'food'
        AND nutrition IS NOT NULL
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
        -- clocked rows only: dedupes the direct-upload channel (its rows
        -- never carry the key and are duplicates on overlap days) and
        -- guarantees the entry clock with no fallback
        AND payload LIKE '%{ENTRY_CLOCK_KEY}%'
    )
    SELECT
      {_hash_expr('d')} AS _userId,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, d.meal_utc) AS meal_time,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, d.entry_utc) AS entry_time,
      d.carb_grams,
      d.absorption_minutes,
      d.meal_time_edited
    FROM deduped d
    INNER JOIN _bm_picked p
      ON d._userId = p._userId
     AND d.meal_utc >= p.first_clock_ts
     AND d.meal_utc <= p.last_clock_ts
    INNER JOIN _bm_tz u ON d._userId = u._userId
    WHERE d.rn = 1
      AND d.carb_grams IS NOT NULL
    ORDER BY 1, 2
    ;
    """

    # User-initiated boluses, HealthKit-flag-FIRST with the ±15 s
    # reason='normalBolus' dosingDecision match as fallback. The DD-only
    # match (the simulator-export pattern) silently drops manual boluses
    # wherever the DD stream is thin -- the first export produced boluses in
    # only a ~2-week window because these users' dosingDecision uploads are
    # sparse. The HK payload flag rides on the bolus row itself, so it does
    # not depend on DD availability; flag=1/true = autobolus (excluded),
    # flag=0/false = manual, absent -> DD fallback, else 'unknown' (excluded,
    # counted in the diagnostic printed at export time).
    bolus_classify_sql = f"""
    --begin-sql
    CREATE OR REPLACE TEMP VIEW _bm_bolus_classified AS
    WITH bolus_rows AS (
      SELECT
        b._userId,
        TRY_CAST(b.time_string AS TIMESTAMP) AS b_utc,
        COALESCE(
          TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE),
          TRY_CAST(b.normal AS DOUBLE)
        ) AS bolus_units,
        -- flag values arrive as '1.0'/'0.0' strings, so classify via numeric
        -- cast (the FDA Method-2 form), with 'true'/'false' as a belt
        LOWER(get_json_object(b.payload,
          "$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']")) AS auto_flag,
        LOWER(get_json_object(b.payload,
          "$['com.loopkit.InsulinKit.MetadataKeyManuallyEntered']")) AS logged_flag
      FROM {BDDP_TABLE} b
      INNER JOIN _bm_picked p
        ON b._userId = p._userId
       AND TRY_CAST(b.time_string AS TIMESTAMP) >= p.first_clock_ts
       AND TRY_CAST(b.time_string AS TIMESTAMP) <= p.last_clock_ts
      WHERE b.type = 'bolus'
        AND b.subType = 'normal'
        AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    ),
    -- one row per physical bolus: re-ingests and dual upload paths (pump +
    -- HealthKit) share (user, time, units); MAX picks up the flag from
    -- whichever copy carries it
    deduped AS (
      SELECT
        _userId, b_utc, bolus_units,
        MAX(auto_flag) AS auto_flag,
        MAX(logged_flag) AS logged_flag
      FROM bolus_rows
      GROUP BY _userId, b_utc, bolus_units
    ),
    nb AS (
      SELECT _userId, TRY_CAST(time_string AS TIMESTAMP) AS nb_ts
      FROM {BDDP_TABLE}
      WHERE type = 'dosingDecision'
        AND reason = 'normalBolus'
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    ),
    matched AS (
      SELECT
        d._userId, d.b_utc, d.bolus_units, d.auto_flag, d.logged_flag,
        MAX(CASE WHEN n.nb_ts IS NOT NULL THEN 1 ELSE 0 END) AS has_nb_dd
      FROM deduped d
      LEFT JOIN nb n
        ON n._userId = d._userId
       AND ABS(TIMESTAMPDIFF(SECOND, n.nb_ts, d.b_utc)) <= 15
      GROUP BY d._userId, d.b_utc, d.bolus_units, d.auto_flag, d.logged_flag
    )
    SELECT *,
      CASE WHEN TRY_CAST(auto_flag AS DOUBLE) = 1 OR auto_flag = 'true'
             THEN 'autobolus_flag'
           WHEN TRY_CAST(auto_flag AS DOUBLE) = 0 OR auto_flag = 'false'
             THEN 'manual_flag'
           WHEN TRY_CAST(logged_flag AS DOUBLE) = 1 OR logged_flag = 'true'
             THEN 'manual_logged'  -- user-LOGGED insulin (not pump-delivered);
                                   -- excluded from bolus_u at MVP, visible in diag
           WHEN has_nb_dd = 1 THEN 'manual_dd'
           ELSE 'unknown' END AS classification
    FROM matched
    ;
    """
    spark.sql(bolus_classify_sql)

    diag = spark.sql(f"""
    --begin-sql
    SELECT {_hash_expr('c')} AS _userId, c.classification, COUNT(*) AS n
    FROM _bm_bolus_classified c
    GROUP BY 1, 2
    ORDER BY 1, 2
    ;
    """).toPandas()
    print("\nBolus classification coverage (subType='normal' rows):")
    print(diag.to_string(index=False))

    boluses_sql = f"""
    --begin-sql
    SELECT
      {_hash_expr('c')} AS _userId,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, c.b_utc) AS bolus_timestamp,
      c.bolus_units
    FROM _bm_bolus_classified c
    INNER JOIN _bm_tz u ON c._userId = u._userId
    WHERE c.classification IN ('manual_flag', 'manual_dd')
    ORDER BY 1, 2
    ;
    """

    # Dosing decisions: reason='loop' gives the ~5-min iob series; 'normalBolus'
    # gives the recommendation the user saw at manual-bolus moments. Values are
    # exported as raw strings -- their shape is pinned during local assembly.
    dosing_sql = f"""
    --begin-sql
    WITH deduped AS (
      SELECT
        _userId,
        TRY_CAST(time_string AS TIMESTAMP) AS dd_utc,
        reason,
        insulinOnBoard,
        recommendedBolus,
        ROW_NUMBER() OVER (
          PARTITION BY _userId, time_string, reason
          ORDER BY created_timestamp DESC
        ) AS rn
      FROM {BDDP_TABLE}
      WHERE type = 'dosingDecision'
        AND reason IN ('loop', 'normalBolus')
        AND TRY_CAST(time_string AS TIMESTAMP) IS NOT NULL
    )
    SELECT
      {_hash_expr('d')} AS _userId,
      TIMESTAMPADD(MINUTE, u.tz_offset_min, d.dd_utc) AS dd_timestamp,
      d.reason,
      d.insulinOnBoard AS insulin_on_board_raw,
      d.recommendedBolus AS recommended_bolus_raw
    FROM deduped d
    INNER JOIN _bm_picked p
      ON d._userId = p._userId
     AND d.dd_utc >= p.first_clock_ts
     AND d.dd_utc <= p.last_clock_ts
    INNER JOIN _bm_tz u ON d._userId = u._userId
    WHERE d.rn = 1
    ORDER BY 1, 2
    ;
    """

    def _fetch(sql):
        return spark.sql(sql).toPandas()

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            name: pool.submit(_fetch, sql)
            for name, sql in [
                ("users", users_sql),
                ("cgm", cgm_sql),
                ("carbs", carbs_sql),
                ("boluses", boluses_sql),
                ("dosing", dosing_sql),
            ]
        }
        frames = {name: fut.result() for name, fut in futures.items()}

    os.makedirs(output_dir, exist_ok=True)
    for name, df in frames.items():
        path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"Wrote {len(df)} rows to {path}")


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_dir", default=OUTPUT_DIR)
    _parser.add_argument("--n_users", type=int, default=N_EXPORT_USERS)
    _args, _ = _parser.parse_known_args()

    run(spark, output_dir=_args.output_dir, n_users=_args.n_users)
