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
SEG3_START = date(2024, 1, 29)
SEG3_END = date(2024, 2, 11)
WINDOW_END = date(2024, 2, 11)

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
    "uploadID",
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
    "`bgTargetPhysicalActivity` string, "
    "`uploadID` string"
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


def _archetype_age_filtered(user_id="int_user_07"):
    """5y M — dropped by the age >= 6 cohort gate (COHORT_WHERE in
    `analysis/utils/data_loading.py`). Full TIR shape so the user would
    otherwise pass every other filter; isolates the age filter as the
    sole reason for exclusion."""
    rows = [_pump_settings_row(user_id)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


# Days within each segment (0-indexed) where int_user_08 / int_user_09 carry
# cbg + override activations. Three days per segment → 864 cbg readings per
# segment, well below 8-1's 2,822-reading coverage threshold, so 08/09 drop
# out of 8-1's cohort while still providing per-activation glucose for 8-2.
_PRESET_ACTIVATION_DAYS = (2, 7, 12)

# Three preset names, each emitted twice per segment so all three satisfy
# the `is_valid_name_only_seg{2,3}` ≥2-per-phase gate. Distributing two
# presets per day across three days (rather than three presets per day on
# fewer days) keeps activation windows non-overlapping (3 h each at 10:00
# and 14:00). The 18 total activations per user give 8-2 three paired
# groups in `_all` after aggregation, clearing build_datasets' ≥3 threshold.
_PRESET_SLOTS = (
    # (day_index_within_segment, hour, preset_name)
    (2, 10, "Workout"),
    (2, 14, "Sleep"),
    (7, 10, "Workout"),
    (7, 14, "Pre-meal"),
    (12, 10, "Sleep"),
    (12, 14, "Pre-meal"),
)


def _emit_workout_segment(user_id, seg_start, day_event_rows_fn):
    """Build 14 days within one segment for int_user_08: events on every day
    (for transition classification), cbg + preset activations on the three
    activation days.
    """
    rows = []
    for d_idx in range(14):
        day = seg_start + timedelta(days=d_idx)
        if d_idx in _PRESET_ACTIVATION_DAYS:
            rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(100.0)))
        for slot_day, slot_hour, preset in _PRESET_SLOTS:
            if slot_day == d_idx:
                rows.append(_override_row(
                    user_id,
                    when=datetime(day.year, day.month, day.day, slot_hour, 0, 0),
                    preset=preset,
                    br_sf=0.7,
                    cr_isf_sf=0.7,
                    duration_seconds=3600,
                ))
        rows.extend(day_event_rows_fn(user_id, day, n_events=20))
    return rows


def _archetype_multi_preset(user_id="int_user_08"):
    """Three preset names (Workout, Sleep, Pre-meal), each with 2 activations
    in seg1, seg2, and seg3 — so every preset satisfies both
    is_valid_name_only_seg2 and is_valid_name_only_seg3. After aggregation,
    8-2 sees three paired groups (one per preset name) in `_all`, clearing
    build_datasets' ≥3 threshold. Sparse cbg (3 days / segment) drops 08 from
    8-1's cohort via the 70% coverage gate.
    """
    rows = [_pump_settings_row(user_id)]
    rows.extend(_emit_workout_segment(user_id, SEG1_START, _temp_basal_day_rows))
    rows.extend(_emit_workout_segment(user_id, SEG2_START, _autobolus_day_rows))
    rows.extend(_emit_workout_segment(user_id, SEG3_START, _autobolus_day_rows))
    return rows


def _archetype_single_preset_ab_only(user_id="int_user_09"):
    """One Workout activation, in seg2 only — fails 8-2's name-validity gate.

    is_valid_name_only_seg2=FALSE (0 seg1 activations); is_valid_name_only_seg3=FALSE.
    Excluded from 8.2b and 8.2c primary tables. Shares 08's sparse-cbg pattern,
    so likewise drops out of 8-1's cohort.
    """
    rows = [_pump_settings_row(user_id)]
    # seg1: TB events + cbg on activation days, but no Workouts (this is the
    # excluded archetype's defining characteristic).
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        if d_idx in _PRESET_ACTIVATION_DAYS:
            rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(100.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    # seg2: one Workout, on the first activation day.
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        if d_idx in _PRESET_ACTIVATION_DAYS:
            rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(100.0)))
        if d_idx == _PRESET_ACTIVATION_DAYS[0]:
            rows.append(_override_row(
                user_id,
                when=datetime(day.year, day.month, day.day, 10, 0, 0),
                preset="Workout",
                br_sf=0.7,
                cr_isf_sf=0.7,
                duration_seconds=3600,
            ))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    # seg3: AB events + cbg on activation days, no Workouts.
    for d_idx in range(14):
        day = SEG3_START + timedelta(days=d_idx)
        if d_idx in _PRESET_ACTIVATION_DAYS:
            rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(100.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


# ---------------------------------------------------------------------------
# Carb-change archetypes (int_user_12, _13, _14 for 8-8)
# ---------------------------------------------------------------------------


_MEAL_HOURS = (8, 13, 19)  # breakfast / lunch / dinner


def _transition_user_with_carbs(user_id, seg1_carbs_per_day, seg2_carbs_per_day):
    """Shared shape: 14-day TB seg1 + 14-day AB seg2 + full CBG + three meal
    rows per day (breakfast / lunch / dinner) splitting the daily total.
    Splitting avoids 8-8's per-entry outlier filter at 150 g (single 180 g
    rows would be dropped). Same TIR target both segments so 8-1's cohort
    sees a stable user."""
    rows = [_pump_settings_row(user_id)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
        for hour in _MEAL_HOURS:
            rows.append(_food_row(user_id, day, hour=hour, carb_grams=seg1_carbs_per_day / 3.0))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
        for hour in _MEAL_HOURS:
            rows.append(_food_row(user_id, day, hour=hour, carb_grams=seg2_carbs_per_day / 3.0))
    return rows


def _archetype_stable_carbs(user_id="int_user_12"):
    """~150 g/day in seg1 and seg2 — 0% change, lands in 8-8 `Consistent` (≤25%) stratum."""
    return _transition_user_with_carbs(user_id, seg1_carbs_per_day=150.0, seg2_carbs_per_day=150.0)


def _archetype_increased_carbs(user_id="int_user_13"):
    """seg1 ~120 g/day → seg2 ~180 g/day (+50%) — 8-8 `Inconsistent / Increased` stratum."""
    return _transition_user_with_carbs(user_id, seg1_carbs_per_day=120.0, seg2_carbs_per_day=180.0)


def _archetype_decreased_carbs(user_id="int_user_14"):
    """seg1 ~180 g/day → seg2 ~120 g/day (-33%) — 8-8 `Inconsistent / Decreased` stratum."""
    return _transition_user_with_carbs(user_id, seg1_carbs_per_day=180.0, seg2_carbs_per_day=120.0)


def _archetype_carb_outlier(user_id="int_user_24"):
    """Single 200 g food entry per day — exceeds 8-8's per-entry ≤150 g
    outlier filter ([analysis_8-8_*.py:107-108]). Every carb row is
    dropped by the filter; the user has no surviving carb data and is
    excluded from 8-8's cohort via the INNER JOIN on
    `valid_transition_carbs`. Full TIR data so 8-1's cohort still
    includes them (and N grows by 1)."""
    rows = [_pump_settings_row(user_id)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
        rows.append(_food_row(user_id, day, hour=12, carb_grams=200.0))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
        rows.append(_food_row(user_id, day, hour=12, carb_grams=200.0))
    return rows


# ---------------------------------------------------------------------------
# Demographic representatives (int_user_15, _16 for 8-5)
# ---------------------------------------------------------------------------


def _transition_user_stable_tir(user_id, tir_pct=75.0):
    """Bare transition user: TB seg1, AB seg2, full CBG at fixed TIR, no overrides.
    Same shape for both demographic reps; bin assignment happens via _DEMOGRAPHICS."""
    rows = [_pump_settings_row(user_id)]
    for d_idx in range(14):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(tir_pct)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    for d_idx in range(14):
        day = SEG2_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(tir_pct)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_demo_child(user_id="int_user_15"):
    """8y M — Children (6–<12) age bin for 8-5; passes 8-1 cohort."""
    return _transition_user_stable_tir(user_id)


def _archetype_demo_senior(user_id="int_user_16"):
    """70y F — Older Adults (≥65) age bin for 8-5; passes 8-1 cohort."""
    return _transition_user_stable_tir(user_id)


# ---------------------------------------------------------------------------
# Stable-AB cohort archetypes (int_user_19, _20 for 8-6)
# ---------------------------------------------------------------------------


def _set_upload_id(rows, upload_id):
    """Post-hoc set uploadID on every row. Used to link a stable-AB user to
    a row in jaeb_upload_to_userid via the BDDP `uploadID` column."""
    for r in rows:
        r["uploadID"] = upload_id
    return rows


def _stable_ab_42_day_user(user_id):
    """42 days of 100% AB starting 2024-06-01 — first-AB day = 2024-06-01,
    stable 14-day window = days 28..41 (2024-06-29 to 2024-07-12). Full CBG
    every day for 70%+ coverage. pumpSettings dated inside the stable window
    so the guardrails staging script can match it."""
    rows = [_pump_settings_row(user_id, setup_day=STABLE_START + timedelta(days=28))]
    for d_idx in range(42):
        day = STABLE_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_stable_ab_jaeb(user_id="int_user_19"):
    """Sustained 100% AB for 42 days + JAEB upload linkage. Lands in 8-6's
    `glycemic_endpoints_by_jaeb_id` output via the INNER JOIN on `uploadID`."""
    rows = _stable_ab_42_day_user(user_id)
    return _set_upload_id(rows, "upload_19")


def _archetype_stable_ab_no_jaeb(user_id="int_user_20"):
    """Sustained 100% AB for 42 days WITHOUT JAEB linkage. Reaches
    `stable_autobolus_segments` and `glycemic_endpoints_stable_autobolus`
    but is excluded from 8-6's per-PtID output (no matching uploadID)."""
    return _stable_ab_42_day_user(user_id)


# ---------------------------------------------------------------------------
# Durability cohort archetypes (int_user_21, _22, _23 for 8-7)
# ---------------------------------------------------------------------------


# Durability window — disjoint from the transition window
# (2024-01-01..2024-02-11) and adjacent to the stable-AB window. 60 days
# gives ≥56 days post-adoption for the sustain/discontinue archetypes;
# 35 days fails the min_followup gate.


def _archetype_adopt_sustain(user_id="int_user_21"):
    """100% AB every day for 60 days. Adoption (rolling 3-day AB% ≥ 80%)
    fires by day 2; final 28-day AB% = 100% → sustained in 8-7's Table 8.7a."""
    rows = [_pump_settings_row(user_id, setup_day=STABLE_START)]
    for d_idx in range(60):
        day = STABLE_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_adopt_discontinue(user_id="int_user_22"):
    """Adopts day 2 (100% AB days 0..29), then discontinues — days 30..59
    have temp-basal events only (AB% = 0%). Rolling 4-week AB% drops below
    20% by day 50 → registers as event in 8-7's KM curve."""
    rows = [_pump_settings_row(user_id, setup_day=STABLE_START)]
    for d_idx in range(30):
        day = STABLE_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    for d_idx in range(30, 60):
        day = STABLE_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_insufficient_followup(user_id="int_user_23"):
    """100% AB every day for 35 days. Adopts day 2 but only 33 days follow-up
    < `min_followup_days` (56) → dropped by `autobolus_durability`'s gate."""
    rows = [_pump_settings_row(user_id, setup_day=STABLE_START)]
    for d_idx in range(35):
        day = STABLE_START + timedelta(days=d_idx)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(75.0)))
        rows.extend(_autobolus_day_rows(user_id, day, n_events=20))
    return rows


def _archetype_day_undercoverage(user_id="int_user_25"):
    """Loop dosing on alternating days only (21 of 42 days, Jan 1 – Feb 11):
    every 14-day window holds exactly 7 dosing days (7/14 = 50%, below the 70%
    day-coverage gate in export_valid_transition_segments.py), while anchors
    from Jan 28 onward still satisfy the 28-day observation bound. The user
    therefore reaches the 6-3a funnel's "Candidate 28-day window" stage but
    drops at "Day-coverage gate" — the only archetype separating those two
    stages. Temp-basal-only and CBG-free, so the user enters no segment table
    and no other analysis cohort (never adopts AB → invisible to 8-7's
    eligible count)."""
    rows = []
    for d_idx in range(0, 42, 2):
        day = SEG1_START + timedelta(days=d_idx)
        rows.extend(_temp_basal_day_rows(user_id, day, n_events=10))
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
    "int_user_07": _archetype_age_filtered,
    "int_user_08": _archetype_multi_preset,
    "int_user_09": _archetype_single_preset_ab_only,
    "int_user_12": _archetype_stable_carbs,
    "int_user_13": _archetype_increased_carbs,
    "int_user_14": _archetype_decreased_carbs,
    "int_user_15": _archetype_demo_child,
    "int_user_16": _archetype_demo_senior,
    "int_user_19": _archetype_stable_ab_jaeb,
    "int_user_20": _archetype_stable_ab_no_jaeb,
    "int_user_21": _archetype_adopt_sustain,
    "int_user_22": _archetype_adopt_discontinue,
    "int_user_23": _archetype_insufficient_followup,
    "int_user_24": _archetype_carb_outlier,
    "int_user_25": _archetype_day_undercoverage,
    # TODO: int_user_07, 10, 11, 17, 18 — see archetypes.md for the full catalog.
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
    "int_user_07": {"gender": "M", "age_years": 5, "yld_years": 1},  # age <6 → dropped
    "int_user_08": {"gender": "F", "age_years": 35, "yld_years": 10},
    "int_user_09": {"gender": "M", "age_years": 28, "yld_years": 6},
    "int_user_12": {"gender": "M", "age_years": 30, "yld_years": 5},
    "int_user_13": {"gender": "F", "age_years": 40, "yld_years": 10},
    "int_user_14": {"gender": "M", "age_years": 25, "yld_years": 3},
    "int_user_15": {"gender": "M", "age_years": 8, "yld_years": 1},   # Children (6–<12) age bin
    "int_user_16": {"gender": "F", "age_years": 70, "yld_years": 30}, # Older Adults (≥65) age bin
    "int_user_19": {"gender": "F", "age_years": 35, "yld_years": 10},
    "int_user_20": {"gender": "M", "age_years": 35, "yld_years": 10},
    "int_user_21": {"gender": "F", "age_years": 30, "yld_years": 8},
    "int_user_22": {"gender": "M", "age_years": 30, "yld_years": 8},
    "int_user_23": {"gender": "F", "age_years": 30, "yld_years": 5},
    "int_user_24": {"gender": "M", "age_years": 30, "yld_years": 5},
    "int_user_25": {"gender": "F", "age_years": 33, "yld_years": 7},
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
    """One upload ↔ PtID row for int_user_19 (the JAEB-linked stable-AB
    archetype). int_user_20 shares the same BDDP shape but has no row in
    this table — 8-6's INNER JOIN on `uploadID` will exclude it.

    Schema matches production: columns are `uploadID`, `PtID` (joined to
    `bddp_sample_all_2.uploadID` per analysis_8-6_*.py:103-105 and
    analysis_8-7_*.py:85-88).
    """
    rows = [{"uploadID": "upload_19", "PtID": "ptid_19"}]
    pdf = pd.DataFrame(rows, columns=["uploadID", "PtID"])
    spark.createDataFrame(
        pdf, schema="`uploadID` string, `PtID` string"
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"Wrote {len(rows)} JAEB linkage row(s) to {table_name}")


def build_user_diagnosis_type(spark, table_name, loop_recommendations_table):
    """One row per synthetic Loop user, resolved diagnosis = 'type1'.

    Mirrors dev.fda_510k_rwd.user_diagnosis_type (data_staging/
    export_user_diagnosis_type.py): the FDA Loop-user universe is the distinct
    _userId in loop_recommendations. The §8 loaders gate every cohort on
    diagnosis_type = 'type1', so marking every synthetic user type1 keeps the
    cohorts at their pre-gate composition. The gate's *exclusion* path
    (non-type1 dropped) is exercised separately by test_type1_diagnosis_gate.py.

    Columns match production: _userId, diagnosis_patients, diagnosis_seagull,
    is_jaeb, diagnosis_type.
    """
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT DISTINCT
            _userId,
            'type1'              AS diagnosis_patients,
            CAST(NULL AS STRING) AS diagnosis_seagull,
            FALSE                AS is_jaeb,
            'type1'              AS diagnosis_type
        FROM {loop_recommendations_table}
    """)
    n = spark.table(table_name).count()
    print(f"Wrote {n} diagnosis row(s) to {table_name} (all type1)")
