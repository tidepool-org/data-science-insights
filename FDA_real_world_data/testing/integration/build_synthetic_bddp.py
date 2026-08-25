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

# IR-1002 (guardrail-group) window: 28 days, disjoint from every other window.
# The length is pinned from both sides. At least 28 days so these users anchor
# a candidate 28-day window and clear the day-coverage gate — test_analysis_6_3a
# pins those two funnel stages EXACTLY against the full Loop-user count, so a
# shorter window silently breaks the §6.3 cohort-flow test. At most ~35 days so
# they still cannot produce a stable-AB segment (needs a fully-AB 14-day window
# starting ≥28 days after first AB, i.e. ≥42 days) or a durability outcome
# (needs ≥56 days follow-up). Being ~all-AB, they also fail the TB→AB validity
# box (seg1 must be <30% AB), so they enter no §8 analysis cohort.
IR1002_START = date(2024, 9, 2)
IR1002_DAYS = 28

# IR-6B (preset dose-response) window: 28 days, disjoint from every other
# fixture window. The length is pinned from both sides exactly like
# IR1002_START: at least 28 days of daily dosing so every user anchors a
# candidate 28-day window with 100% dosing-day coverage (test_analysis_6_3a
# pins those two funnel stages exactly against the full Loop-user count), and
# at most ~35 days so no stable-AB segment (needs a fully-AB 14-day window
# starting ≥28 days after first AB) and no durability outcome (needs ≥56 days
# follow-up; test_analysis_8_7 pins eligible users at exactly 2) can form.
# All-AB from day 0, so no TB→AB transition segment forms either (seg1 would
# be 100% AB; the validity box needs <30%) — these users reach only the
# IR-1002 universes (as additional guardrail-group members; every IR-2/IR-3
# assertion is per-user or relative) and IR-6B's C2 series.
IR6B_START = date(2024, 11, 4)
IR6B_DAYS = 28

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
    "activeSchedule",
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
    "`activeSchedule` string, "
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


def _cbg_rows_with_gaps(user_id, day, mgdl_values, version=DEFAULT_VERSION):
    """Like _cbg_rows, but a None entry emits NO reading at that index.

    Used by IR-6B archetypes to knock readings out of a specific clock span
    (e.g. the 30-minute starting-glucose lookback) while the rest of the day
    keeps the exact 5-minute cadence. Still demands a full 288-slot list so
    the index↔time-of-day arithmetic stays explicit at the call site.
    """
    if len(mgdl_values) != 288:
        raise ValueError(f"need 288 cbg slots per day, got {len(mgdl_values)}")
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
        if mgdl_values[i] is not None
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
    bg_target_low_mgdl=100.0,
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
        activeSchedule="Default",
        bgTargets=json.dumps({
            "Default": schedule_entry(
                low=bg_target_low_mgdl / 18.018,
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
    """One override activation. `target_low_mgdl=None` emits a preset with NO
    own target range (the IR-1002 mitigation-fallback case, where the effective
    lower bound comes from the scheduled correction range);
    `duration_seconds=None` emits an indefinite override."""
    bg_target = None
    if target_low_mgdl is not None and target_high_mgdl is not None:
        bg_target = json.dumps({
            "low": target_low_mgdl / 18.018,
            "high": target_high_mgdl / 18.018,
        })
    return _row(
        _userId=user_id,
        time_string=_iso(when),
        overridePreset=preset,
        basalRateScaleFactor=br_sf,
        carbRatioScaleFactor=cr_isf_sf,
        insulinSensitivityScaleFactor=cr_isf_sf,
        bgTarget=bg_target,
        duration=None if duration_seconds is None else str(duration_seconds),
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
# IR-1002 archetypes (guardrail groups; PLN IR-1002 §7.2/§7.3)
# ---------------------------------------------------------------------------
#
# Each user runs IR1002_DAYS days of autobolus days (10 automated boluses/day,
# clearing the >=3 AB-day threshold) with full-coverage CBG, inside the short
# IR1002 window. Overrides are placed to land the user in one guardrail group.
# Guardrail bounds under test: target within [67, 250] mg/dL, insulin needs
# within [15%, 200%]; mitigation = needs > 170% with an effective target lower
# bound < 110 mg/dL.


def _ir1002_day_rows(user_id, n_days=IR1002_DAYS, tb_day_idxs=(), start=IR1002_START):
    """Full-coverage CBG plus dosing events for the IR-1002 window. Day indices
    in `tb_day_idxs` emit temp-basal days (NOT eligible AB days) — used to break
    a multiday override's span and to precede a user's first AB day."""
    rows = []
    for i in range(n_days):
        day = start + timedelta(days=i)
        rows.extend(_cbg_rows(user_id, day, _cbg_day_at_target_tir(100.0)))
        if i in tb_day_idxs:
            rows.extend(_temp_basal_day_rows(user_id, day, n_events=10))
        else:
            rows.extend(_autobolus_day_rows(user_id, day, n_events=10))
    return rows


def _ir1002_override(user_id, day_idx, hour=10, start=IR1002_START, **kwargs):
    day = start + timedelta(days=day_idx)
    return _override_row(
        user_id,
        when=datetime(day.year, day.month, day.day, hour, 0, 0),
        **kwargs,
    )


def _archetype_ir1002_compliant(user_id="int_user_26"):
    """Two in-guardrail activations (needs 100%, target 100–120) → `compliant`."""
    rows = [_pump_settings_row(user_id, setup_day=IR1002_START)]
    rows.extend(_ir1002_day_rows(user_id))
    for i in (2, 5):
        rows.append(_ir1002_override(user_id, i, preset="Compliant"))
    return rows


def _archetype_ir1002_p_violator(user_id="int_user_27"):
    """Target low 40 mg/dL — below the 67 mg/dL preset guardrail; needs in
    bounds and never above the mitigation threshold → `p_only`."""
    rows = [_pump_settings_row(user_id, setup_day=IR1002_START)]
    rows.extend(_ir1002_day_rows(user_id))
    rows.append(_ir1002_override(
        user_id, 3, preset="LowTarget", target_low_mgdl=40.0, target_high_mgdl=120.0,
    ))
    return rows


def _archetype_ir1002_m_fallback(user_id="int_user_28"):
    """Needs 180% with NO preset target, so the effective lower bound comes from
    the scheduled correction range (100 < 110 mg/dL) → `m_only` via the
    settings fallback — the path that needs correction_range_history."""
    rows = [_pump_settings_row(
        user_id, setup_day=IR1002_START, bg_target_low_mgdl=100.0,
    )]
    rows.extend(_ir1002_day_rows(user_id))
    rows.append(_ir1002_override(
        user_id, 4, preset="HighNeeds", br_sf=1.8, cr_isf_sf=round(1 / 1.8, 4),
        target_low_mgdl=None, target_high_mgdl=None,
    ))
    return rows


def _archetype_ir1002_pre_first_ab(user_id="int_user_29"):
    """A P-violating activation on day 0 — a temp-basal day, BEFORE the user's
    first eligible AB day — plus a compliant one after. The violation is not
    qualifying (PLN §7.3 anchor), so the user is `compliant`."""
    rows = [_pump_settings_row(user_id, setup_day=IR1002_START)]
    rows.extend(_ir1002_day_rows(user_id, tb_day_idxs=(0,)))
    rows.append(_ir1002_override(
        user_id, 0, preset="PreAB", target_low_mgdl=40.0, target_high_mgdl=120.0,
    ))
    rows.append(_ir1002_override(user_id, 4, preset="Compliant"))
    return rows


def _archetype_ir1002_multiday_span(user_id="int_user_30"):
    """A compliant activation on day 4 running 14 h into day 5, which is a
    temp-basal day — so `is_all_days_ab` is FALSE and IR-3 drops the activation
    while IR-2 still classifies the user (`compliant`). A same-day activation on
    day 8 stays in IR-3's set."""
    rows = [_pump_settings_row(user_id, setup_day=IR1002_START)]
    rows.extend(_ir1002_day_rows(user_id, tb_day_idxs=(5,)))
    rows.append(_ir1002_override(
        user_id, 4, hour=20, preset="Overnight", duration_seconds=14 * 3600,
    ))
    rows.append(_ir1002_override(user_id, 8, preset="Compliant"))
    return rows


def _archetype_ir1002_indeterminate(user_id="int_user_31"):
    """Needs 180%, no preset target, and NO pumpSettings record at all — the
    effective lower bound is unresolvable → `is_m_indeterminate`. The user lands
    in `compliant` with depends_on_indeterminate TRUE."""
    rows = _ir1002_day_rows(user_id)
    rows.append(_ir1002_override(
        user_id, 3, preset="HighNeedsNoSettings", br_sf=1.8,
        cr_isf_sf=round(1 / 1.8, 4), target_low_mgdl=None, target_high_mgdl=None,
    ))
    return rows


def _archetype_ir1002_both(user_id="int_user_32"):
    """Two separate violations — a P-violating target (40 mg/dL) on day 2 and an
    M-violating combination (needs 180% with its own target low 100 < 110) on
    day 6 → `both`. Neither activation violates both bounds by itself, so this
    also exercises the union-across-activations rollup."""
    rows = [_pump_settings_row(user_id, setup_day=IR1002_START)]
    rows.extend(_ir1002_day_rows(user_id))
    rows.append(_ir1002_override(
        user_id, 2, preset="LowTarget", target_low_mgdl=40.0, target_high_mgdl=120.0,
    ))
    rows.append(_ir1002_override(
        user_id, 6, preset="HighNeeds", br_sf=1.8, cr_isf_sf=round(1 / 1.8, 4),
        target_low_mgdl=100.0, target_high_mgdl=130.0,
    ))
    return rows


# ---------------------------------------------------------------------------
# IR-6B dose-response archetypes (int_user_33..40; analysis_ir-6b)
# ---------------------------------------------------------------------------
#
# Every user runs IR6B_DAYS all-autobolus days (10 automated boluses/day) in
# the disjoint IR6B window — see the IR6B_START comment for why exactly 28
# days and why all-AB. Preset activations build known IR-6B episodes: an
# episode's outcome window is [t0 - 1h, t0 + duration + 3h) and the CGM grid
# is 5-minute, so index = (hour*60 + minute) / 5 within a day:
#
#   09:00 → 108    09:30 → 114    09:55 → 119    10:00 → 120
#   12:00 → 144    12:30 → 150    13:00 → 156    15:00 → 180
#
# A 10:00 + 1h activation spans indices 108..167 (60 readings; pre 108..119,
# during 120..131, post 132..167); a 10:00 + 2h activation spans 108..179
# (72 readings). The 09:55 reading (index 119) is the starting-glucose anchor
# for a 10:00 activation. Engineered values stay ≥2 mg/dL clear of the
# 38/70/180/250/500 band edges (the mmol round-trip is inexact) and ≥54 so no
# hypo events form anywhere in the IR-6B fixture.


def _ir6b_dosing_rows(user_id):
    """10 automated boluses on each of the IR6B_DAYS days: every day is an
    eligible AB day (≥3 automated boluses, version 3.2.0, adult), and
    dosing-day coverage is 100% for the 6-3a candidate-window funnel."""
    rows = []
    for day_index in range(IR6B_DAYS):
        day = IR6B_START + timedelta(days=day_index)
        rows.extend(_autobolus_day_rows(user_id, day, n_events=10))
    return rows


def _archetype_ir6b_dose_response(user_id="int_user_33"):
    """Four C2 activations pinning IR-6B's per-episode band counts, the
    count-pooled user cell, the starting-glucose bins and the missing-anchor
    exclusion (test_analysis_ir_6b cases 1-3). All at 10:00, own target
    100–120 (midpoint 110), on separate days so filter 3 never binds.

    day 2  A1  needs 50%,  1 h: the six 09:30–09:55 readings are 62 (so the
               anchor is 62 → bin <70) and six post readings are 200 →
               n_below/n_in/n_above = 6/48/6 of 60 (TB70 10%, TIR 80%, TAR 10%).
    day 5  A2  needs 150%, 1 h: six post readings at 200 → 0/54/6 of 60
               (TAR 10%); anchor 100 → bin 70-180.
    day 8  A3  needs 150%, 2 h: six pre readings at 260 (anchor 260 → bin
               >250; also n_above250 = 6) + twelve post readings at 200 →
               0/54/18 of 72 (TAR 25%). Pooled with A2 at the 150% level:
               TAR = (6+18)*100/132 = 18.18%, deliberately distinct from the
               mean of the two episode TARs (10+25)/2 = 17.5 — pinning
               reading-count pooling over percentage averaging.
    day 11 A4  needs 90%,  1 h: the 09:30–09:55 readings are NOT emitted, so
               no reading falls in the 30-minute lookback → filter 6 drops
               the episode while its 54 of 60 window readings (90%) still
               clear the 70% coverage gate — the anchor alone is missing.
               Level 90 must therefore appear in no user cell.
    """
    rows = _ir6b_dosing_rows(user_id)

    # day 2 — A1 (needs 50%)
    values = [100.0] * 288
    for index in range(114, 120):   # 09:30–09:55: six 62s in the pre hour
        values[index] = 62.0
    for index in range(150, 156):   # 12:30–12:55: six 200s in the post arm
        values[index] = 200.0
    rows.extend(_cbg_rows(user_id, IR6B_START + timedelta(days=2), values))
    rows.append(_ir1002_override(
        user_id, 2, start=IR6B_START, preset="NeedsHalf",
        br_sf=0.5, cr_isf_sf=2.0, duration_seconds=3600,
    ))

    # day 5 — A2 (needs 150%, 1 h)
    values = [100.0] * 288
    for index in range(150, 156):   # 12:30–12:55: six 200s in the post arm
        values[index] = 200.0
    rows.extend(_cbg_rows(user_id, IR6B_START + timedelta(days=5), values))
    rows.append(_ir1002_override(
        user_id, 5, start=IR6B_START, preset="NeedsOneFifty",
        br_sf=1.5, cr_isf_sf=round(1 / 1.5, 4), duration_seconds=3600,
    ))

    # day 8 — A3 (needs 150%, 2 h — a longer window at the same level)
    values = [100.0] * 288
    for index in range(114, 120):   # 09:30–09:55: six 260s (anchor >250)
        values[index] = 260.0
    for index in range(150, 162):   # 12:30–13:25: twelve 200s in the post arm
        values[index] = 200.0
    rows.extend(_cbg_rows(user_id, IR6B_START + timedelta(days=8), values))
    rows.append(_ir1002_override(
        user_id, 8, start=IR6B_START, preset="NeedsOneFifty",
        br_sf=1.5, cr_isf_sf=round(1 / 1.5, 4), duration_seconds=7200,
    ))

    # day 11 — A4 (needs 90%, no starting-glucose anchor)
    values = [100.0] * 288
    for index in range(114, 120):   # 09:30–09:55: no readings at all
        values[index] = None
    rows.extend(_cbg_rows_with_gaps(user_id, IR6B_START + timedelta(days=11), values))
    rows.append(_ir1002_override(
        user_id, 11, start=IR6B_START, preset="NeedsNinety",
        br_sf=0.9, cr_isf_sf=round(1 / 0.9, 4), duration_seconds=3600,
    ))
    return rows


def _archetype_ir6b_filter_mechanics(user_id="int_user_34"):
    """Filter-3/filter-4 mechanics plus the indefinite-truncated episode
    (test_analysis_ir_6b case 4). The day-3/6/7/20 activations run at needs
    80% and the day-13/14 boundary pair at needs 60%, so each concept owns
    its own exposure level.

    day 3       two compliant activations (10:00 and 13:00, 1 h each) → both
                fail filter 3 (a second activation of any kind spoils the
                user-day).
    days 6/7    22:00 + 1 h, then 01:00 + 1 h the next day: the gap from the
                day-6 capped end (23:00) to the day-7 start (01:00) is 2 h,
                under the 4 h full-disjointness floor — the two hypothetical
                windows overlap on [00:00, 02:00), so filter 4 (settled
                2026-08-25, MC: SYMMETRIC) drops BOTH. The pre-tightening
                rule kept the day-7 episode; this pair is the regression
                case for the symmetric drop.
    days 13/14  the boundary pair (plan 9.A case 4): 20:00 + 1 h, then
                01:00 + 1 h the next day — exactly 4 h from the capped end
                (21:00) to the next start, so the hypothetical windows TOUCH
                at day-14 midnight ([19:00, 00:00) then [00:00, 05:00)) and
                half-open disjointness keeps BOTH.
    day 20      10:00 with duration NULL (indefinite): no later override and
                dosing ends day 27, so the staged effective duration is the
                end-of-data clip — midnight after the last dosing day minus
                t0 = 7 d 14 h = 655,200 s > 24 h → is_truncated. The episode
                keeps its pre hour and first 24 h and gets NO post arm:
                window = [09:00 day 20, 10:00 day 21), n_pre/n_during/n_post
                = 12/288/0 of 300 readings.

    CBG: flat 100 on days 13, 14, 20 and 21 only — exactly the days the
    surviving windows touch (non-survivors never reach the CGM join).
    """
    rows = _ir6b_dosing_rows(user_id)
    for day_index in (13, 14, 20, 21):
        rows.extend(_cbg_rows(user_id, IR6B_START + timedelta(days=day_index),
                              [100.0] * 288))
    rows.append(_ir1002_override(
        user_id, 3, hour=10, start=IR6B_START, preset="PairFirst",
        br_sf=0.8, cr_isf_sf=1.25, duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 3, hour=13, start=IR6B_START, preset="PairSecond",
        br_sf=0.8, cr_isf_sf=1.25, duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 6, hour=22, start=IR6B_START, preset="CrowdedPrior",
        br_sf=0.8, cr_isf_sf=1.25, duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 7, hour=1, start=IR6B_START, preset="CrowdedNext",
        br_sf=0.8, cr_isf_sf=1.25, duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 13, hour=20, start=IR6B_START, preset="BoundaryFirst",
        br_sf=0.6, cr_isf_sf=round(1 / 0.6, 4), duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 14, hour=1, start=IR6B_START, preset="BoundarySecond",
        br_sf=0.6, cr_isf_sf=round(1 / 0.6, 4), duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 20, hour=10, start=IR6B_START, preset="Indefinite",
        br_sf=0.8, cr_isf_sf=1.25, duration_seconds=None,
    ))
    return rows


def _archetype_ir6b_c2_exclusions(user_id="int_user_35"):
    """C2 exclusion paths (test_analysis_ir_6b case 5): three activations,
    only the third a C2 member.

    day 2  P violation: own target low 40 < 67 mg/dL (needs 100%, in bounds).
    day 5  M violation: needs 180% with own target low 100 < 110; needs stays
           within [15%, 200%] so this is NOT also a P violation (mirrors
           int_user_32's day-6 activation).
    day 8  compliant at needs 120%: the user's only C2 episode.

    The violating activations are qualifying and all-days-AB, so their
    exclusion is attributable to the violation flags alone — and because
    IR-6B's candidate set requires (in_c1 OR in_c2), they never enter the
    episode frame at all; the test pins their staged flags directly. This
    user lands in guardrail group `both` for IR-2 (not asserted there;
    the IR-2 partition check is relative)."""
    rows = _ir6b_dosing_rows(user_id)
    rows.extend(_cbg_rows(user_id, IR6B_START + timedelta(days=8), [100.0] * 288))
    rows.append(_ir1002_override(
        user_id, 2, start=IR6B_START, preset="LowTarget",
        target_low_mgdl=40.0, target_high_mgdl=120.0, duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 5, start=IR6B_START, preset="HighNeeds",
        br_sf=1.8, cr_isf_sf=round(1 / 1.8, 4),
        target_low_mgdl=100.0, target_high_mgdl=130.0, duration_seconds=3600,
    ))
    rows.append(_ir1002_override(
        user_id, 8, start=IR6B_START, preset="Compliant120",
        br_sf=1.2, cr_isf_sf=round(1 / 1.2, 4), duration_seconds=3600,
    ))
    return rows


def _archetype_ir6b_line_level_user(user_id):
    """Minimal C2 contributor for the line-eligibility gate
    (test_analysis_ir_6b case 6): one compliant 1 h activation on day 3 at
    needs 50%, flat-100 CBG on that day only. Five of these (int_user_36..40)
    plus int_user_33's day-2 activation put six users at the 50% level
    (≥ MIN_USERS_FOR_LINE = 5), while every other needs level in the fixture
    carries at most three users."""
    rows = _ir6b_dosing_rows(user_id)
    rows.extend(_cbg_rows(user_id, IR6B_START + timedelta(days=3), [100.0] * 288))
    rows.append(_ir1002_override(
        user_id, 3, start=IR6B_START, preset="NeedsHalf",
        br_sf=0.5, cr_isf_sf=2.0, duration_seconds=3600,
    ))
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
    # IR-1002 guardrail groups (PLN IR-1002). Short, disjoint window — see
    # IR1002_START. never_preset is covered by the existing archetypes above,
    # which have AB days and no overrides in that window.
    "int_user_26": _archetype_ir1002_compliant,
    "int_user_27": _archetype_ir1002_p_violator,
    "int_user_28": _archetype_ir1002_m_fallback,
    "int_user_29": _archetype_ir1002_pre_first_ab,
    "int_user_30": _archetype_ir1002_multiday_span,
    "int_user_31": _archetype_ir1002_indeterminate,
    "int_user_32": _archetype_ir1002_both,
    # IR-6B dose-response (analysis_ir-6b). All-AB disjoint window — see
    # IR6B_START. These users also flow into the IR-1002 universes (the
    # IR-2/IR-3 cohorts grow, which is safe: every IR-2/IR-3 assertion is
    # per-user or relative) and into 6-3a's Loop-user stages (relative too).
    "int_user_33": _archetype_ir6b_dose_response,
    "int_user_34": _archetype_ir6b_filter_mechanics,
    "int_user_35": _archetype_ir6b_c2_exclusions,
    # Five interchangeable single-activation users sharing the 50% needs
    # level; the lambdas exist only to bind each user id to the shared builder.
    "int_user_36": lambda: _archetype_ir6b_line_level_user("int_user_36"),
    "int_user_37": lambda: _archetype_ir6b_line_level_user("int_user_37"),
    "int_user_38": lambda: _archetype_ir6b_line_level_user("int_user_38"),
    "int_user_39": lambda: _archetype_ir6b_line_level_user("int_user_39"),
    "int_user_40": lambda: _archetype_ir6b_line_level_user("int_user_40"),
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
    # IR-1002 guardrail-group archetypes — all adults, so the age gate never
    # binds and group membership is decided purely by preset configuration.
    "int_user_26": {"gender": "F", "age_years": 34, "yld_years": 9},
    "int_user_27": {"gender": "M", "age_years": 41, "yld_years": 12},
    "int_user_28": {"gender": "F", "age_years": 29, "yld_years": 6},
    "int_user_29": {"gender": "M", "age_years": 37, "yld_years": 11},
    "int_user_30": {"gender": "F", "age_years": 45, "yld_years": 20},
    "int_user_31": {"gender": "M", "age_years": 52, "yld_years": 24},
    "int_user_32": {"gender": "F", "age_years": 31, "yld_years": 8},
    # IR-6B dose-response archetypes — all adults so the age gate never binds
    # (dob is back-computed from SEG1_START, so ages read ~1y older by the
    # IR6B window; still nowhere near the <6 cutoff).
    "int_user_33": {"gender": "F", "age_years": 36, "yld_years": 12},
    "int_user_34": {"gender": "M", "age_years": 42, "yld_years": 18},
    "int_user_35": {"gender": "F", "age_years": 27, "yld_years": 5},
    "int_user_36": {"gender": "M", "age_years": 33, "yld_years": 9},
    "int_user_37": {"gender": "F", "age_years": 39, "yld_years": 14},
    "int_user_38": {"gender": "M", "age_years": 48, "yld_years": 22},
    "int_user_39": {"gender": "F", "age_years": 26, "yld_years": 4},
    "int_user_40": {"gender": "M", "age_years": 55, "yld_years": 30},
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
    cohorts at their pre-gate composition. Because of this, the per-analysis
    integration tests pass whether or not a given loader actually applies the
    gate — the gate's *exclusion* path is therefore exercised separately:
    test_type1_diagnosis_gate.py pins the helper (load_type1_user_ids), and
    test_type1_gate_wired_in_loaders.py pins that every cohort loader CALLS it
    (a no-type1 diagnosis table must empty every analysis cohort).

    Columns match production: _userId, diagnosis_patients, diagnosis_seagull,
    is_jaeb, is_lada, diagnosis_type.
    """
    spark.sql(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT DISTINCT
            _userId,
            'type1'              AS diagnosis_patients,
            CAST(NULL AS STRING) AS diagnosis_seagull,
            FALSE                AS is_jaeb,
            FALSE                AS is_lada,
            'type1'              AS diagnosis_type
        FROM {loop_recommendations_table}
    """)
    n = spark.table(table_name).count()
    print(f"Wrote {n} diagnosis row(s) to {table_name} (all type1)")
