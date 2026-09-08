"""Export Loop's DISPLAYED forecast (dosingDecision.bgForecast) at the analysis horizons for the cohort-B users.

Run on Databricks; Claude does not execute this. Same candidates table, user pick, pseudonym salt and per-user
timezone handling as behavior_model/data_staging/export_behavior_traces.py, so `loop_forecasts.csv` joins the
trace CSVs. Download it to behavior_model/data/behavior_traces_b/.

Every dosing decision in the trace windows with a non-null forecast (any reason; the `reason` column is kept, and the
local loader uses reason 'loop'), deduped to the latest ingest per user, time_string and reason, carries the forecast
Loop computed at that moment: an array of (time, value) points at 5-minute
steps from the latest glucose sample. The full arrays are large, so this script keeps only the grid points nearest
to the starting glucose's time + 30 / 60 / 90 / 120 / 180 min, plus the starting glucose, plus the point count and the BG units.
Diagnostics come first: the column's type, rows and non-null forecasts by decision reason, and a 300-row raw sample
written to loop_forecasts_raw_sample.csv (always, even when parsing fails). Confirmed shape (2026-09-04):
[{"time": {"$date": iso}, "value": v}] with values in mmol/L and points on the 5-minute wall-clock grid starting with the
glucose sample Loop used. Times and values are pulled out with regexp_extract_all (from_json could not take the "$date"
key) and zipped into points. Each horizon column is the grid point nearest to (glucose sample time + horizon), where the glucose sample time is
the first forecast point -- the reading Loop started from, which the local frame files under its own 5-min bucket --
so forecast and outcome share one origin reading; forecast_0 is that starting glucose. A one-row trace and per-stage counts are printed so a failure shows where it happens. Values are exported in the record's own BG units (see bg_units);
the local loader converts mmol/L to mg/dL.

Output columns: _userId, glucose_timestamp (user-local; the reading Loop started from), decision_timestamp, reason, bg_units, n_points,
                forecast_0, forecast_5 … forecast_30, forecast_60, forecast_90, forecast_120, forecast_180, forecast_240, forecast_300, forecast_360
"""
import argparse
import os

BDDP_TABLE = "dev.default.bddp_sample_all_2"
CANDIDATES_TABLE = "dev.fda_510k_rwd.behavior_trace_candidates_b"
OUTPUT_DIR = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
    "/forecast_uncertainty/data_staging/exports"
)
USERID_SALT = "behavior-model-v1"   # MUST equal export_behavior_traces.USERID_SALT
N_EXPORT_USERS = 10                 # MUST equal export_behavior_traces.N_EXPORT_USERS
OVERRIDE_USER_IDS = []
HORIZONS_MIN = (0, 5, 10, 15, 20, 25, 30, 60, 90, 120, 180, 240, 300, 360)
MATCH_TOLERANCE_MIN = 2.5      # the grid point nearest to (glucose time + horizon) is within half a step


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
    print(f"Exporting Loop forecasts for {len(picked)} user(s); raw ids stay on Databricks.")

    spark.sql(f"""
    SELECT b._userId,
           CAST(MAX_BY(b.timezoneOffset, TRY_CAST(b.time_string AS TIMESTAMP)) AS INT) AS tz_offset_min
    FROM {BDDP_TABLE} b INNER JOIN _fu_picked p ON b._userId = p._userId
    WHERE b.timezoneOffset IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    GROUP BY b._userId
    """).createOrReplaceTempView("_fu_tz")

    # --- diagnostics first: where do forecasts live, and what do they look like? ---
    schema = {f.name: f.dataType.simpleString() for f in spark.table(BDDP_TABLE).schema.fields}
    forecast_column = next((c for c in ("bgForecast", "bgforecast", "predictedGlucose") if c in schema), None)
    if forecast_column is None:
        raise RuntimeError(f"no forecast column in {BDDP_TABLE}; dosingDecision-looking columns: "
                           f"{[c for c in schema if 'bg' in c.lower() or 'forecast' in c.lower() or 'predict' in c.lower()]}")
    forecast_type = schema[forecast_column]
    raw_expr = f"CAST(b.{forecast_column} AS STRING)" if forecast_type == "string" else f"to_json(b.{forecast_column})"
    print(f"forecast column: {forecast_column} ({forecast_type}); exported via {raw_expr}")

    spark.sql(f"""
    SELECT b.reason, COUNT(*) AS rows, SUM(CASE WHEN b.{forecast_column} IS NOT NULL THEN 1 ELSE 0 END) AS with_forecast
    FROM {BDDP_TABLE} b INNER JOIN _fu_picked p ON b._userId = p._userId
      AND TRY_CAST(b.time_string AS TIMESTAMP) >= p.first_clock_ts AND TRY_CAST(b.time_string AS TIMESTAMP) <= p.last_clock_ts
    WHERE b.type = 'dosingDecision' GROUP BY b.reason ORDER BY rows DESC
    """).show(20, truncate=False)

    # One row per dosing decision (any reason) in the windows with a non-null forecast, deduped to the latest ingest.
    spark.sql(f"""
    WITH deduped AS (
      SELECT b._userId, TRY_CAST(b.time_string AS TIMESTAMP) AS dd_utc, b.reason,
             {raw_expr} AS forecast_raw,
             COALESCE(get_json_object(CAST(b.units AS STRING), '$.bg'), 'mg/dL') AS bg_units,
             ROW_NUMBER() OVER (PARTITION BY b._userId, b.time_string, b.reason ORDER BY b.created_timestamp DESC) AS rn
      FROM {BDDP_TABLE} b
      INNER JOIN _fu_picked p ON b._userId = p._userId
        AND TRY_CAST(b.time_string AS TIMESTAMP) >= p.first_clock_ts
        AND TRY_CAST(b.time_string AS TIMESTAMP) <= p.last_clock_ts
      WHERE b.type = 'dosingDecision' AND b.{forecast_column} IS NOT NULL
        AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    )
    SELECT * FROM deduped WHERE rn = 1
    """).createOrReplaceTempView("_fu_dd")

    os.makedirs(output_dir, exist_ok=True)
    sample = spark.sql(f"SELECT {_hash_expr('d')} AS _userId, d.reason, d.bg_units, d.forecast_raw FROM _fu_dd d LIMIT 300").toPandas()
    sample.to_csv(os.path.join(output_dir, "loop_forecasts_raw_sample.csv"), index=False)
    print(f"raw sample of {len(sample)} forecasts written to loop_forecasts_raw_sample.csv; two examples (truncated):")
    for raw in sample["forecast_raw"].head(2):
        print("  ", str(raw)[:400])

    # Parse WITHOUT from_json: pull every point's time and value out of the JSON text with regular expressions
    # (the "$date" key defeated from_json's schema strings), zip them into points, and match each horizon to the
    # point at (decision's 5-minute bucket + horizon). The first point is the glucose Loop started from.
    # A one-row trace of every intermediate step is printed so a failure shows where it happens.
    TIME_RE = 'date":"([^"]+)"'            # matches the value after "$date": (or a plain "date":)
    VALUE_RE = '"value":([-0-9.eE]+)'
    # get(array, i) is 0-based and returns NULL out of range (element_at raises on Databricks); trace a rich row.
    trace = spark.sql(f"""
    SELECT substr(forecast_raw, 1, 120) AS raw_head,
           size(regexp_extract_all(forecast_raw, '{TIME_RE}', 1)) AS n_times,
           size(regexp_extract_all(forecast_raw, '{VALUE_RE}', 1)) AS n_values,
           get(regexp_extract_all(forecast_raw, '{TIME_RE}', 1), 1) AS second_time_text,
           TRY_CAST(get(regexp_extract_all(forecast_raw, '{TIME_RE}', 1), 1) AS TIMESTAMP) AS second_time_cast,
           TRY_CAST(get(regexp_extract_all(forecast_raw, '{VALUE_RE}', 1), 1) AS DOUBLE) AS second_value,
           dd_utc, from_unixtime(floor(unix_timestamp(dd_utc) / 300) * 300) AS bucket_utc
    FROM _fu_dd ORDER BY size(regexp_extract_all(forecast_raw, '{TIME_RE}', 1)) DESC LIMIT 1
    """).toPandas()
    print("trace of the decision with the most forecast points:")
    print(trace.T.to_string())
    print("distribution of points per forecast (share of decisions):")
    spark.sql(f"""
    SELECT size(regexp_extract_all(forecast_raw, '{TIME_RE}', 1)) AS n_points, COUNT(*) AS decisions
    FROM _fu_dd GROUP BY 1 ORDER BY decisions DESC LIMIT 8
    """).show()

    spark.sql(f"""
    WITH extracted AS (
      SELECT _userId, dd_utc, reason, bg_units,
             regexp_extract_all(forecast_raw, '{TIME_RE}', 1) AS times,
             regexp_extract_all(forecast_raw, '{VALUE_RE}', 1) AS values,
             TRY_CAST(get(regexp_extract_all(forecast_raw, '{TIME_RE}', 1), 0) AS TIMESTAMP) AS glucose_utc
      FROM _fu_dd
    ),
    exploded AS (
      SELECT _userId, dd_utc, reason, bg_units, glucose_utc, size(values) AS n_points,
             posexplode(arrays_zip(times, values)) AS (position, pt)
      FROM extracted
      WHERE size(times) > 1 AND size(times) = size(values) AND glucose_utc IS NOT NULL
    ),
    points AS (
      SELECT _userId, dd_utc, reason, bg_units, glucose_utc, n_points, position,
             TRY_CAST(pt.times AS TIMESTAMP) AS point_time, TRY_CAST(pt.values AS DOUBLE) AS value
      FROM exploded
    ),
    horizons AS (
      SELECT p._userId, p.dd_utc, p.reason, p.bg_units, p.glucose_utc, p.n_points, h.horizon, p.value,
             ABS(unix_timestamp(p.point_time) - unix_timestamp(p.glucose_utc) - 60 * h.horizon) AS gap_s,
             ROW_NUMBER() OVER (PARTITION BY p._userId, p.dd_utc, p.reason, h.horizon
                                ORDER BY ABS(unix_timestamp(p.point_time) - unix_timestamp(p.glucose_utc) - 60 * h.horizon)) AS rn
      FROM points p CROSS JOIN (SELECT explode(array(%s)) AS horizon) h
      WHERE h.horizon > 0 AND p.point_time IS NOT NULL
    ),
    starts AS (
      SELECT _userId, dd_utc, reason, value AS forecast_0 FROM points WHERE position = 0
    )
    SELECT h._userId, h.dd_utc, h.glucose_utc, h.reason, h.bg_units, MAX(h.n_points) AS n_points, MAX(s.forecast_0) AS forecast_0,
           %s
    FROM horizons h LEFT JOIN starts s ON s._userId = h._userId AND s.dd_utc = h.dd_utc AND s.reason = h.reason
    WHERE h.rn = 1 AND h.gap_s <= %d
    GROUP BY h._userId, h.dd_utc, h.glucose_utc, h.reason, h.bg_units
    """ % (", ".join(str(h) for h in HORIZONS_MIN),
           ", ".join(f"MAX(CASE WHEN horizon = {h} THEN value END) AS forecast_{h}" for h in HORIZONS_MIN if h > 0),
           int(MATCH_TOLERANCE_MIN * 60))
    ).createOrReplaceTempView("_fu_forecasts")

    stage = spark.sql(f"""
    SELECT (SELECT COUNT(*) FROM _fu_dd) AS decisions,
           (SELECT COUNT(*) FROM _fu_dd WHERE size(regexp_extract_all(forecast_raw, '{TIME_RE}', 1)) > 0) AS with_times,
           (SELECT COUNT(*) FROM _fu_dd WHERE size(regexp_extract_all(forecast_raw, '{TIME_RE}', 1)) = size(regexp_extract_all(forecast_raw, '{VALUE_RE}', 1))) AS times_match_values,
           (SELECT COUNT(*) FROM _fu_dd WHERE TRY_CAST(get(regexp_extract_all(forecast_raw, '{TIME_RE}', 1), 1) AS TIMESTAMP) IS NOT NULL) AS second_time_casts
    """).toPandas().iloc[0]
    print("stage counts:", stage.to_dict())

    counts = spark.sql("""
    SELECT (SELECT COUNT(*) FROM _fu_dd) AS decisions,
           (SELECT COUNT(*) FROM _fu_forecasts) AS parsed,
           (SELECT COUNT(*) FROM _fu_forecasts WHERE forecast_180 IS NOT NULL) AS with_180
    """).toPandas().iloc[0]
    print(f"decisions {counts['decisions']}, parsed {counts['parsed']} "
          f"({counts['parsed'] / max(counts['decisions'], 1):.1%}), with a 180-min point {counts['with_180']}")

    out = spark.sql(f"""
    SELECT {_hash_expr('f')} AS _userId,
           TIMESTAMPADD(MINUTE, u.tz_offset_min, f.glucose_utc) AS glucose_timestamp,
           TIMESTAMPADD(MINUTE, u.tz_offset_min, f.dd_utc) AS decision_timestamp,
           f.reason, f.bg_units, f.n_points, {', '.join(f'f.forecast_{h}' for h in HORIZONS_MIN)}
    FROM _fu_forecasts f INNER JOIN _fu_tz u ON f._userId = u._userId
    ORDER BY 1, 2
    """).toPandas()
    if out.empty:
        print("NO FORECASTS PARSED. The raw sample file shows the field's shape; paste two lines of it back to adapt the parser.")
    path = os.path.join(output_dir, "loop_forecasts.csv")
    out.to_csv(path, index=False)
    print(f"Wrote {len(out)} rows to {path}; bg_units seen: {sorted(out['bg_units'].dropna().unique().tolist())}")
    if len(out):
        print("median forecast_0 by bg_units (a value near 5-10 means mmol/L whatever the label says):")
        print(out.groupby("bg_units")["forecast_0"].median().round(2).to_string())


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_dir", default=OUTPUT_DIR)
    _parser.add_argument("--n_users", type=int, default=N_EXPORT_USERS)
    _args, _ = _parser.parse_known_args()
    run(spark, output_dir=_args.output_dir, n_users=_args.n_users)
