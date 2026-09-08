"""Export pumpSettings (insulin sensitivity, carb ratio, basal, insulin model) for the cohort-B users.

Run on Databricks (Workspace Run button or a notebook cell); Claude does not execute this. Companion
to behavior_model/data_staging/export_behavior_traces.py: SAME candidates table, SAME user pick
(lowest selection_rank, N_EXPORT_USERS), SAME salted-SHA-256 pseudonym so the ids join the trace CSVs.
Raw ids never leave Databricks.

Emits ONE CSV, settings_raw.csv, to OUTPUT_DIR; download it to behavior_model/data/behavior_traces_b/
next to the five trace CSVs. Values are exported as raw JSON strings (same convention as dosing.csv):
their shape is parsed locally by forecast_uncertainty/model/settings.py::parse_raw_settings, which handles
Loop's [{start: ms-since-midnight, amount}] lists, multi-schedule {name: [...]} maps, and units.bg in
mmol/L. The whole record is exported (not just the trace window) because the setting in force at the
start of a window is the latest record BEFORE it.

Columns: _userId, effective_time (user-local, from time_string + the per-user tz offset), created_time,
active_schedule, then <field>_raw for every candidate field present in the table (missing ones are
omitted, and the discovered column list is printed first -- paste it back if parsing fails). When a
field exists under both its singular and plural spelling, the two are COALESCEd into the one column.
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
OVERRIDE_USER_IDS = []              # raw _userIds; set to skip the sampled pick

# pumpSettings fields worth having, in (table column, csv column) form; either spelling may exist.
CANDIDATE_FIELDS = [
    ("insulinSensitivity", "insulin_sensitivity_raw"), ("insulinSensitivities", "insulin_sensitivity_raw"),
    ("carbRatio", "carb_ratio_raw"), ("carbRatios", "carb_ratio_raw"),
    ("basalSchedules", "basal_schedules_raw"),
    ("bgTarget", "bg_target_raw"), ("bgTargets", "bg_target_raw"),
    ("units", "units_raw"),
    ("insulinModel", "insulin_model_raw"),
]


def _hash_expr(alias):
    return (f"concat('u', substr(sha2(concat({alias}._userId, "
            f"'{USERID_SALT}'), 256), 1, 16))")


def run(spark, output_dir=OUTPUT_DIR, n_users=N_EXPORT_USERS):
    candidates = spark.sql(f"SELECT * FROM {CANDIDATES_TABLE} ORDER BY selection_rank").toPandas()
    picked = (candidates[candidates["_userId"].isin(OVERRIDE_USER_IDS)] if OVERRIDE_USER_IDS
              else candidates.head(n_users))
    if picked.empty:
        raise RuntimeError("no users picked -- check CANDIDATES_TABLE / OVERRIDE_USER_IDS")
    spark.createDataFrame(picked[["_userId"]]).createOrReplaceTempView("_fu_picked")
    print(f"Exporting settings for {len(picked)} user(s); raw ids stay on Databricks.")

    # Both spellings of a field can coexist in the table: Tidepool Loop (TidepoolKit) uploads the plural
    # {schedule name: [...]} maps (insulinSensitivities, carbRatios, bgTargets); other uploaders the singular
    # lists. Every spelling present is read and COALESCEd into one CSV column, in CANDIDATE_FIELDS order,
    # so a record carrying either spelling reaches settings_raw.csv.
    schema = {f.name: f.dataType.simpleString() for f in spark.table(BDDP_TABLE).schema.fields}
    expressions_by_csv_name = {}
    for column, csv_name in CANDIDATE_FIELDS:
        if column in schema:
            expr = f"b.{column}" if schema[column] == "string" else f"to_json(b.{column})"
            expressions_by_csv_name.setdefault(csv_name, []).append(expr)
            print(f"  {column}: {schema[column]}")
    selected = [(exprs[0] if len(exprs) == 1 else f"COALESCE({', '.join(exprs)})") + f" AS {csv_name}"
                for csv_name, exprs in expressions_by_csv_name.items()]
    if "activeSchedule" in schema:
        selected.append("b.activeSchedule AS active_schedule")
    if not selected:
        raise RuntimeError("none of the candidate pumpSettings fields exist in the table")

    tz_sql = f"""
    SELECT b._userId,
           CAST(MAX_BY(b.timezoneOffset, TRY_CAST(b.time_string AS TIMESTAMP)) AS INT) AS tz_offset_min
    FROM {BDDP_TABLE} b INNER JOIN _fu_picked p ON b._userId = p._userId
    WHERE b.timezoneOffset IS NOT NULL AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    GROUP BY b._userId
    """
    spark.sql(tz_sql).createOrReplaceTempView("_fu_tz")

    settings_sql = f"""
    WITH deduped AS (
      SELECT b.*, TRY_CAST(b.time_string AS TIMESTAMP) AS settings_utc,
             ROW_NUMBER() OVER (PARTITION BY b._userId, b.time_string ORDER BY b.created_timestamp DESC) AS rn
      FROM {BDDP_TABLE} b INNER JOIN _fu_picked p ON b._userId = p._userId
      WHERE b.type = 'pumpSettings' AND TRY_CAST(b.time_string AS TIMESTAMP) IS NOT NULL
    )
    SELECT {_hash_expr('b')} AS _userId,
           TIMESTAMPADD(MINUTE, u.tz_offset_min, b.settings_utc) AS effective_time,
           b.created_timestamp AS created_time,
           {', '.join(selected)}
    FROM deduped b INNER JOIN _fu_tz u ON b._userId = u._userId
    WHERE b.rn = 1
    ORDER BY 1, 2
    """
    settings = spark.sql(settings_sql).toPandas()
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "settings_raw.csv")
    settings.to_csv(path, index=False)
    print(f"Wrote {len(settings)} pumpSettings rows to {path}")
    if "units_raw" in settings.columns:
        print("units values seen:", settings["units_raw"].dropna().unique()[:5])


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output_dir", default=OUTPUT_DIR)
    _parser.add_argument("--n_users", type=int, default=N_EXPORT_USERS)
    _args, _ = _parser.parse_known_args()
    run(spark, output_dir=_args.output_dir, n_users=_args.n_users)
