"""Build a deterministic BDDP-shaped synthetic table for integration tests.

`build(spark, bddp_table)` writes one Unity Catalog table whose rows mimic
`dev.default.bddp_sample_all_2`. Each `_userId` is a named archetype that
exercises a specific cohort-filter branch or analysis condition; see
`archetypes.md` for the catalog.

Auxiliary tables (`bddp_user_dates`, `user_gender`, `jaeb_upload_to_userid`)
are written by `build_user_dates(...)`, `build_user_gender(...)`,
`build_jaeb_link(...)`. Each integration test calls these via
`run_pipeline.run(spark)`.

All transition users span 28 days starting `2024-01-01`. Stable-AB and
durability users span their own windows. Loop version defaults to `3.2.0`
(below the 3.4.0 cohort cutoff). cbg readings are at exact 5-minute cadence
to satisfy the 70%-coverage gate (>=2,822 readings per 14-day segment).
"""

import json
from datetime import date, datetime, timedelta

import pandas as pd


SEG1_START = date(2024, 1, 1)
SEG1_END = date(2024, 1, 14)
SEG2_START = date(2024, 1, 15)
SEG2_END = date(2024, 1, 28)
WINDOW_END = date(2024, 1, 28)

# Stable-AB and durability windows live later so they don't collide with
# transition fixtures (a single _userId could in principle have both, but
# we keep them disjoint per-user for clarity).
STABLE_START = date(2024, 6, 1)
DURABILITY_ADOPT_DAY = date(2024, 6, 15)

DEFAULT_VERSION = "3.2.0"
MMOL_PER_MGDL = 1.0 / 18.018
TZ_OFFSET_MIN = -300  # UTC-5 (EST); single TZ for all synthetic users

# Every BDDP column the staging scripts read. Rows are dicts; we fill missing
# columns with None and pass BDDP_SCHEMA explicitly to createDataFrame so
# Spark preserves columns that are all-None across the fixture (Databricks
# Connect's pandas→Arrow path silently drops all-None columns when the schema
# is inferred; see databricks_connect_all_none_drop memo).
BDDP_COLUMNS = [
    "_userId",
    "time_string",
    "created_timestamp",
    "timezoneOffset",
    "type",
    "subType",
    "reason",
    "value",
    "normal",
    "recommendedBolus",
    "recommendedBasal",
    "origin",
    "payload",
    "nutrition",
    "food",
    "overridePreset",
    "basalRateScaleFactor",
    "carbRatioScaleFactor",
    "insulinSensitivityScaleFactor",
    "bgTarget",
    "duration",
    "basalSchedules",
    "bgTargets",
    "insulinSensitivities",
    "insulinSensitivity",
    "carbRatios",
    "carbRatio",
    "basal",
    "bolus",
    "bgSafetyLimit",
    "bgTargetPreprandial",
    "bgTargetPhysicalActivity",
]

BDDP_SCHEMA = (
    "`_userId` string, "
    "`time_string` string, "
    "`created_timestamp` string, "
    "`timezoneOffset` bigint, "
    "`type` string, "
    "`subType` string, "
    "`reason` string, "
    "`value` double, "
    "`normal` double, "
    "`recommendedBolus` string, "
    "`recommendedBasal` string, "
    "`origin` string, "
    "`payload` string, "
    "`nutrition` string, "
    "`food` string, "
    "`overridePreset` string, "
    "`basalRateScaleFactor` double, "
    "`carbRatioScaleFactor` double, "
    "`insulinSensitivityScaleFactor` double, "
    "`bgTarget` string, "
    "`duration` string, "
    "`basalSchedules` string, "
    "`bgTargets` string, "
    "`insulinSensitivities` string, "
    "`insulinSensitivity` string, "
    "`carbRatios` string, "
    "`carbRatio` string, "
    "`basal` string, "
    "`bolus` string, "
    "`bgSafetyLimit` double, "
    "`bgTargetPreprandial` string, "
    "`bgTargetPhysicalActivity` string"
)


def _row(**fields):
    """Build one BDDP row dict with every column populated (None as default)."""
    base = {c: None for c in BDDP_COLUMNS}
    base.update(fields)
    base.setdefault("timezoneOffset", TZ_OFFSET_MIN)
    if "time_string" in fields and base.get("created_timestamp") is None:
        base["created_timestamp"] = base["time_string"]
    return base


def _iso(dt):
    """ISO-8601 with 'Z' suffix; what bddp_sample_all_2.time_string carries."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _origin(version, source_name=None):
    payload = {"version": version}
    if source_name:
        payload["payload"] = {"sourceRevision": {"source": {"name": source_name}}}
    return json.dumps(payload)


_AUTO_PAYLOAD = json.dumps(
    {"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued": 1}
)


# ---------------------------------------------------------------------------
# Per-event row emitters
# ---------------------------------------------------------------------------


def _cbg_rows(user_id, day, mgdl_values, version=DEFAULT_VERSION):
    """288 readings × 1 day at 5-min cadence; mgdl_values must have len == 288."""
    if len(mgdl_values) != 288:
        raise ValueError(f"need 288 cbg values per day, got {len(mgdl_values)}")
    base_dt = datetime(day.year, day.month, day.day, 0, 0, 0)
    return [
        _row(
            _userId=user_id,
            time_string=_iso(base_dt + timedelta(minutes=5 * i)),
            type="cbg",
            value=mgdl_values[i] * MMOL_PER_MGDL,
            origin=_origin(version),
        )
        for i in range(288)
    ]


def _autobolus_day_rows(user_id, day, n_events=10, version=DEFAULT_VERSION):
    """Emit n_events autobolus pairs (loop DD + smb bolus 2s later) on `day`."""
    rows = []
    base = datetime(day.year, day.month, day.day, 6, 0, 0)
    for i in range(n_events):
        t = base + timedelta(minutes=30 * i)
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t),
            type="dosingDecision",
            reason="loop",
            origin=_origin(version),
        ))
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t + timedelta(seconds=2)),
            type="bolus",
            subType="smb",
            normal=0.5,
            origin=_origin(version),
        ))
    return rows


def _temp_basal_day_rows(user_id, day, n_events=10, version=DEFAULT_VERSION):
    rows = []
    base = datetime(day.year, day.month, day.day, 6, 0, 0)
    for i in range(n_events):
        t = base + timedelta(minutes=30 * i)
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t),
            type="dosingDecision",
            reason="loop",
            origin=_origin(version),
        ))
        rows.append(_row(
            _userId=user_id,
            time_string=_iso(t + timedelta(seconds=2)),
            type="basal",
            origin=_origin(version),
        ))
    return rows


def _correction_bolus_rows(user_id, day, hour, units):
    """User-initiated correction bolus: normal-subType bolus + nearby normalBolus DD.

    The export_loop_recommendations.py logic excludes these from autobolus
    counting (directional-match + ±15s normalBolus exclusion), so they don't
    affect day classification.
    """
    t = datetime(day.year, day.month, day.day, hour, 0, 0)
    return [
        _row(
            _userId=user_id,
            time_string=_iso(t),
            type="dosingDecision",
            reason="normalBolus",
            origin=_origin(DEFAULT_VERSION),
        ),
        _row(
            _userId=user_id,
            time_string=_iso(t + timedelta(seconds=2)),
            type="bolus",
            subType="normal",
            normal=units,
            origin=_origin(DEFAULT_VERSION),
        ),
    ]


def _food_row(user_id, day, hour, carb_grams, absorption_minutes=180):
    t = datetime(day.year, day.month, day.day, hour, 0, 0)
    return _row(
        _userId=user_id,
        time_string=_iso(t),
        type="food",
        nutrition=json.dumps({
            "carbohydrate": {"net": carb_grams, "units": "grams"},
            "estimatedAbsorptionDuration": absorption_minutes * 60,
        }),
    )


def _pump_settings_row(
    user_id,
    setup_day=SEG1_START,
    bg_target_high_mgdl=120.0,
    isf_mgdl_per_u=50.0,
    cir_g_per_u=15.0,
    basal_u_per_hr=0.5,
):
    """Single pumpSettings record at the start of the user's window.

    All schedules are constant 24h ('Default') so analyses see one segment.
    Use bg_target_high_mgdl > 180 to trigger a guardrail violation.

    setup_day must fall inside the transition segment window because
    export_segments_within_guardrails.py inner-joins pumpSettings to segments
    with TRY_CAST(time_string AS DATE) BETWEEN seg1_start AND seg2_end —
    settings dated before seg1_start are silently dropped from the guardrails
    table (which silently passes guardrail-violator archetypes through).
    """
    t = datetime(setup_day.year, setup_day.month, setup_day.day, 0, 0, 0)
    schedule_entry = lambda **kv: [{"start": 0, **kv}]  # noqa: E731
    return _row(
        _userId=user_id,
        time_string=_iso(t),
        type="pumpSettings",
        basalSchedules=json.dumps({
            "Default": schedule_entry(rate=basal_u_per_hr),
        }),
        bgTargets=json.dumps({
            "Default": schedule_entry(
                low=100.0 / 18.018,
                high=bg_target_high_mgdl / 18.018,
            ),
        }),
        insulinSensitivities=json.dumps({
            "Default": schedule_entry(amount=isf_mgdl_per_u / 18.018),
        }),
        carbRatios=json.dumps({
            "Default": schedule_entry(amount=cir_g_per_u),
        }),
        basal=json.dumps({"rateMaximum": {"value": 5.0}}),
        bolus=json.dumps({"amountMaximum": {"value": 10.0}}),
        bgSafetyLimit=80.0 / 18.018,
        bgTargetPreprandial=json.dumps({
            "Default": schedule_entry(low=85.0 / 18.018, high=110.0 / 18.018),
        }),
        bgTargetPhysicalActivity=json.dumps({
            "Default": schedule_entry(low=130.0 / 18.018, high=180.0 / 18.018),
        }),
    )


def _override_row(
    user_id, when, preset, br_sf=1.0, cr_isf_sf=1.0,
    target_low_mgdl=100.0, target_high_mgdl=120.0, duration_seconds=3600,
):
    return _row(
        _userId=user_id,
        time_string=_iso(when),
        overridePreset=preset,
        basalRateScaleFactor=br_sf,
        carbRatioScaleFactor=cr_isf_sf,
        insulinSensitivityScaleFactor=cr_isf_sf,
        bgTarget=json.dumps({
            "low": target_low_mgdl / 18.018,
            "high": target_high_mgdl / 18.018,
        }),
        duration=str(duration_seconds),
    )


# ---------------------------------------------------------------------------
# CBG distribution helpers
# ---------------------------------------------------------------------------


def _cbg_day_at_target_tir(tir_pct, hypo_idxs=()):
    """Return 288 mg/dL values where exactly tir_pct% are 100 (in range).

    Remaining readings split: half at 200 (>180), half at 60 (<70). hypo_idxs
    overrides the listed indices to 50 (<54) — used for hypo-event archetypes.
    """
    in_range_n = int(round(288 * tir_pct / 100.0))
    high_n = (288 - in_range_n) // 2
    low_n = 288 - in_range_n - high_n
    values = (
        [100.0] * in_range_n
        + [200.0] * high_n
        + [60.0] * low_n
    )
    for i in hypo_idxs:
        values[i] = 50.0
    return values


def _cbg_day_with_hypo_event():
    """288 readings shaped to produce exactly 1 fully-formed hypo event.

    A hypo event is 3 consecutive <54 followed by 3 consecutive >70 (exit).
    Indices 100..102 -> 50 mg/dL (hypo entry); 110..112 -> 100 mg/dL (exit);
    everything else 100 mg/dL.
    """
    values = [100.0] * 288
    values[100] = values[101] = values[102] = 50.0
    return values


# ---------------------------------------------------------------------------
# Archetype builders (transition cohort)
# ---------------------------------------------------------------------------


def _archetype_tir_improver(user_id="int_user_01", version=DEFAULT_VERSION):
    """seg1 TIR ~50%, seg2 TIR ~75%; passes all filters. (8-1, 8-3, 8-5, 8-8)"""
    rows = [_pump_settings_row(user_id)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(50.0), version))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20, version=version))
        rows.append(_food_row(user_id, day, hour=12, carb_grams=50.0))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0), version))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20, version=version))
        rows.append(_food_row(user_id, day, hour=12, carb_grams=50.0))
    return rows


def _archetype_tir_decliner(user_id="int_user_02"):
    """seg1 TIR 75%, seg2 TIR 62.5% (autobolus over-corrects). (8-1, 8-5)

    Both targets must be representable exactly as (in-range / 288)
    so per-day TIR matches the per-segment mean used in 8-1's assertions:
    216/288 = 75.0; 180/288 = 62.5. 60.0 was the previous target but
    288 × 0.60 = 172.8 rounds to 173, producing TIR=60.0694%.
    """
    rows = [_pump_settings_row(user_id)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(62.5)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_hypo_event_user(user_id="int_user_03"):
    """seg1: 1 hypo event; seg2: 0 hypo events. (8-1)"""
    rows = [_pump_settings_row(user_id)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        cbg_values = _cbg_day_with_hypo_event() if d_idx == 0 else _cbg_day_at_target_tir(80.0)
        rows.extend(_cbg_rows(user_id, day, cbg_values))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(80.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_version_filtered(user_id="int_user_04"):
    """Loop version 3.5.0 — should be DROPPED by the cohort filter. (8-1 cohort check)"""
    return _archetype_tir_improver(user_id=user_id, version="3.5.0")


def _archetype_cbg_undercoverage(user_id="int_user_05"):
    """seg1 has only 7 days of cbg coverage (~2,016 readings; below 2,822). (8-1 cohort check)"""
    rows = [_pump_settings_row(user_id)]
    # 7 of 14 days have cbg => 7 * 288 = 2,016 < 2,822 threshold
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        if d_idx < 7:
            rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(70.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(70.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_guardrail_violator(user_id="int_user_06"):
    """pumpSettings has bg_target_high = 200 (>180) — violates correction-range guardrail."""
    rows = [_pump_settings_row(user_id, bg_target_high_mgdl=200.0)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(70.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(70.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


# ---------------------------------------------------------------------------
# Top-level fixture composition
# ---------------------------------------------------------------------------


# Keys are functions (each returns rows for one user). Driving the build with a
# dict-of-callables makes it trivial to add archetypes incrementally.
ARCHETYPES = {
    "int_user_01": _archetype_tir_improver,
    "int_user_02": _archetype_tir_decliner,
    "int_user_03": _archetype_hypo_event_user,
    "int_user_04": _archetype_version_filtered,
    "int_user_05": _archetype_cbg_undercoverage,
    "int_user_06": _archetype_guardrail_violator,
    # TODO: int_user_07..23 — see archetypes.md for the full catalog.
}


def _build_bddp_rows():
    rows = []
    for user_id, builder in ARCHETYPES.items():
        rows.extend(builder())
    return rows


def build(spark, bddp_table):
    """Materialize the synthetic BDDP fixture as a Unity Catalog table."""
    rows = _build_bddp_rows()
    (
        spark.createDataFrame(rows, schema=BDDP_SCHEMA)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(bddp_table)
    )
    print(f"Wrote {len(rows):,} BDDP rows ({len(ARCHETYPES)} users) to {bddp_table}")


# ---------------------------------------------------------------------------
# Auxiliary tables (demographics, JAEB linkage)
# ---------------------------------------------------------------------------


# Demographics for each archetype. dob is back-computed from the desired age at
# SEG1_START so analysis 8-5's age binning lands the user in the right bucket.
_DEMOGRAPHICS = {
    "int_user_01": {"gender": "F", "age_years": 30, "yld_years": 5},
    "int_user_02": {"gender": "M", "age_years": 45, "yld_years": 15},
    "int_user_03": {"gender": "M", "age_years": 12, "yld_years": 3},
    "int_user_04": {"gender": "F", "age_years": 20, "yld_years": 8},
    "int_user_05": {"gender": "M", "age_years": 50, "yld_years": 20},
    "int_user_06": {"gender": "F", "age_years": 65, "yld_years": 30},
}


def build_user_dates(spark, table_name):
    rows = []
    for user_id, d in _DEMOGRAPHICS.items():
        dob = SEG1_START - timedelta(days=int(365.25 * d["age_years"]))
        diagnosis_date = SEG1_START - timedelta(days=int(365.25 * d["yld_years"]))
        rows.append({
            "userid": user_id,
            "dob": dob,
            "diagnosis_date": diagnosis_date,
        })
    pdf = pd.DataFrame(rows, columns=["userid", "dob", "diagnosis_date"])
    spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable(table_name)
    print(f"Wrote {len(rows)} demographic rows to {table_name}")


def build_user_gender(spark, table_name):
    rows = [{"userid": uid, "gender": d["gender"]} for uid, d in _DEMOGRAPHICS.items()]
    pdf = pd.DataFrame(rows, columns=["userid", "gender"])
    spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable(table_name)
    print(f"Wrote {len(rows)} gender rows to {table_name}")


def build_jaeb_link(spark, table_name):
    """Empty by default; analyses 8-6 / 8-7 will populate when their archetypes land."""
    pdf = pd.DataFrame(columns=["_userId", "PtID"])
    spark.createDataFrame(
        pdf, schema="`_userId` string, `PtID` string"
    ).write.mode("overwrite").saveAsTable(table_name)
    print(f"Wrote 0 JAEB linkage rows to {table_name}")
