"""Export the controller's insulin delivery for the cohort-B users: basal segments and automated boluses.

Run on Databricks; Claude does not execute this. Companion to behavior_model/data_staging/export_behavior_traces.py
(SAME candidates table, pick, pseudonym salt and per-user timezone handling) and to
no_meal_announcement/data_staging/export_user_day_tdd.py, whose BDDP basal semantics this follows:
  * Loop uploads every basal segment twice. HealthKit stream (origin source 'Loop', deliveryType 'temp'): `rate` is
    the DELIVERED rate. Loop-direct stream (origin.name 'com.loopkit.Loop', deliveryType 'automated'): `rate` is the
    COMMANDED rate and `payload.deliveredUnits` is what was delivered. Never sum the two streams; the local code picks
    one per user-day (HealthKit preferred).
  * `suppressed` is the JSON of the scheduled basal the segment replaced; its `rate` is the scheduled rate, so
    delivered − scheduled × duration is the controller's net-of-schedule insulin. `duration` is milliseconds.
  * Autoboluses are bolus rows with subType 'automated', or subType 'normal' carrying the InsulinKit
    "automatically issued" flag. `normal` is the delivered units. Mirrored under both origins: deduped.
  * BDDP re-ingests records many times: everything is deduped to one row per (user, minute, value).

Emits two CSVs to OUTPUT_DIR, download both to behavior_model/data/behavior_traces_b/ next to the trace CSVs:
  basal.csv       : _userId, basal_timestamp, stream ('healthkit' | 'loop_direct'), delivery_type, rate_u_per_h,
                    duration_ms, delivered_units (payload, Loop-direct only), scheduled_rate_u_per_h
  autoboluses.csv : _userId, bolus_timestamp, bolus_units, source ('subtype_automated' | 'automated_flag')
Timestamps follow the other trace CSVs (TIMESTAMPADD of the per-user offset), so run_residuals.py treats them alike.
"""
import argparse
import os
from concurrent.futures import ThreadPoolExecutor

BDDP_TABLE = "dev.default.bddp_sample_all_2"
CANDIDATES_TABLE = "dev.fda_510k_rwd.behavior_trace_candidates_b"
OUTPUT_DIR = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
    "/forecast_uncertainty/data_staging/exports"
)
USERID_SALT = "behavior-model-v1"   # MUST equal export_behavior_traces.USERID_SALT
N_EXPORT_USERS = 10                 # MUST equal export_behavior_traces.N_EXPORT_USERS
OVERRIDE_USER_IDS = []

HEALTHKIT_PREDICATE = "get_json_object(b.origin, '$.payload.sourceRevision.source.name') = 'Loop'"
LOOP_DIRECT_PREDICATE = "get_json_object(b.origin, '$.name') = 'com.loopkit.Loop'"
AUTO_FLAG = "LOWER(get_json_object(b.payload, \"$['com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued']\"))"


def _hash_expr(alias):
    return (f"concat('u', substr(sha2(concat({alias}._userId, "
            f"'{USERID_SALT}'), 256), 1, 16))")


def run(spark, output_dir=OUTPUT_DIR, n_users=N_EXPORT_USERS):
    candidates = spark.sql(f"SELECT * FROM {CANDIDATES_TABLE} ORDER BY selection_rank").toPandas()
    picked = (candidates[candidates["_userId"].isin(OVERRIDE_USER_IDS)] if OVERRIDE_USER_IDS
              else candidates.head(n_users))
    if picked.empty:
        raise RuntimeError("no users picked -- check CANDIDATES_TABLE / OVERRIDE_USER_IDS")
    spark.createDataFrame(picked[["_userId", "first_clock_ts", "last_clock_ts"]]).createOrReplaceTempView("_fu_picked")
    print(f"Exporting insulin delivery for {len(picked)} user(s); raw ids stay on Databricks.")

    spark.sql(f"""
    SELECT b._userId,
           CAST(MAX_BY(b.timezoneOffset, TRY_CAST(b.time_string AS TIMESTAMP)) AS INT) AS tz_offset_min
    FROM {BDDP_TABLE} b INNER JOIN _fu_picked p ON b._userId = p._userId
    WHERE b.timezoneOffset IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    GROUP BY b._userId
    """).createOrReplaceTempView("_fu_tz")

    # Basal segments from both streams, deduped per (user, minute, rate, duration), window-restricted with a
    # one-day margin before the window so the insulin history at the window start is complete.
    basal_sql = f"""
    WITH rows AS (
      SELECT b._userId, TRY_CAST(b.time_string AS TIMESTAMP) AS b_utc,
             CASE WHEN {HEALTHKIT_PREDICATE} THEN 'healthkit'
                  WHEN {LOOP_DIRECT_PREDICATE} THEN 'loop_direct' ELSE 'other' END AS stream,
             b.deliveryType AS delivery_type,
             TRY_CAST(b.rate AS DOUBLE) AS rate_u_per_h,
             TRY_CAST(b.duration AS DOUBLE) AS duration_ms,
             TRY_CAST(get_json_object(b.payload, '$.deliveredUnits') AS DOUBLE) AS delivered_units,
             TRY_CAST(get_json_object(b.suppressed, '$.rate') AS DOUBLE) AS scheduled_rate_u_per_h,
             ROW_NUMBER() OVER (
               PARTITION BY b._userId,
                            CAST(ROUND(unix_timestamp(TRY_CAST(b.time_string AS TIMESTAMP)) / 60.0) AS BIGINT),
                            b.rate, b.duration,
                            CASE WHEN {HEALTHKIT_PREDICATE} THEN 'healthkit'
                                 WHEN {LOOP_DIRECT_PREDICATE} THEN 'loop_direct' ELSE 'other' END
               ORDER BY b.created_timestamp DESC) AS rn
      FROM {BDDP_TABLE} b
      INNER JOIN _fu_picked p ON b._userId = p._userId
        AND TRY_CAST(b.time_string AS TIMESTAMP) >= TIMESTAMPADD(DAY, -1, p.first_clock_ts)
        AND TRY_CAST(b.time_string AS TIMESTAMP) <= p.last_clock_ts
      WHERE b.type = 'basal' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    )
    SELECT {_hash_expr('r')} AS _userId,
           TIMESTAMPADD(MINUTE, u.tz_offset_min, r.b_utc) AS basal_timestamp,
           r.stream, r.delivery_type, r.rate_u_per_h, r.duration_ms, r.delivered_units, r.scheduled_rate_u_per_h
    FROM rows r INNER JOIN _fu_tz u ON r._userId = u._userId
    WHERE r.rn = 1 AND r.stream <> 'other'
    ORDER BY 1, 2
    """

    autobolus_sql = f"""
    WITH rows AS (
      SELECT b._userId, TRY_CAST(b.time_string AS TIMESTAMP) AS b_utc,
             COALESCE(TRY_CAST(get_json_object(b.normal, '$.value') AS DOUBLE), TRY_CAST(b.normal AS DOUBLE)) AS bolus_units,
             CASE WHEN b.subType = 'automated' THEN 'subtype_automated' ELSE 'automated_flag' END AS source,
             ROW_NUMBER() OVER (
               PARTITION BY b._userId,
                            CAST(ROUND(unix_timestamp(TRY_CAST(b.time_string AS TIMESTAMP)) / 60.0) AS BIGINT),
                            b.normal
               ORDER BY b.created_timestamp DESC) AS rn
      FROM {BDDP_TABLE} b
      INNER JOIN _fu_picked p ON b._userId = p._userId
        AND TRY_CAST(b.time_string AS TIMESTAMP) >= TIMESTAMPADD(DAY, -1, p.first_clock_ts)
        AND TRY_CAST(b.time_string AS TIMESTAMP) <= p.last_clock_ts
      WHERE b.type = 'bolus' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
        AND (b.subType = 'automated'
             OR (b.subType = 'normal' AND (TRY_CAST({AUTO_FLAG} AS DOUBLE) = 1 OR {AUTO_FLAG} = 'true')))
    )
    SELECT {_hash_expr('r')} AS _userId,
           TIMESTAMPADD(MINUTE, u.tz_offset_min, r.b_utc) AS bolus_timestamp,
           r.bolus_units, r.source
    FROM rows r INNER JOIN _fu_tz u ON r._userId = u._userId
    WHERE r.rn = 1 AND r.bolus_units > 0
    ORDER BY 1, 2
    """

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {"basal": pool.submit(lambda: spark.sql(basal_sql).toPandas()),
                   "autoboluses": pool.submit(lambda: spark.sql(autobolus_sql).toPandas())}
        frames = {name: fut.result() for name, fut in futures.items()}
    os.makedirs(output_dir, exist_ok=True)
    for name, df in frames.items():
        path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"Wrote {len(df)} rows to {path}")
    if len(frames["basal"]):
        print("basal rows by stream and delivery type:")
        print(frames["basal"].groupby(["stream", "delivery_type"]).size().to_string())
        print("share of temp/automated rows with a scheduled rate:",
              round(frames["basal"].loc[frames["basal"]["delivery_type"] != "scheduled", "scheduled_rate_u_per_h"].notna().mean(), 3))


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_dir", default=OUTPUT_DIR)
    _parser.add_argument("--n_users", type=int, default=N_EXPORT_USERS)
    _args, _ = _parser.parse_known_args()
    run(spark, output_dir=_args.output_dir, n_users=_args.n_users)
