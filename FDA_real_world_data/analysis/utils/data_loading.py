"""Shared loaders for the FDA real-world-data analyses.

Since 2026-11-05 (execution plan 2026-10-27, Step 3) every loader reads a
staged cohort table (data_staging/export_transition_cohort.py,
export_transition_activation_cohort.py, export_stable_cohort.py, and the
cohort columns of export_autobolus_durability.py), re-derives the table's
predicate columns from its criterion columns, asserts they agree, and only
then selects the rows the predicate admits and the diagnosis groups asked
for. No loader applies a gate of its own, so an analysis cohort and its flow
table (utils/cohort_flow.py) are counted from the same columns.

Every loader is split in two: a pure `prepare_<population>` that takes the
table as a pandas frame (coerce the numbers, assert, select the rows and the
groups) and is testable without Spark, and `load_<population>`, which reads
the table and hands it to the pure half. The PLN-1008 no-meal-announcement
population (the NMA analyses) is the last section of this module: its
cohort table is one row per user-day, and its analyses run locally off a
CSV snapshot of it as well as on Databricks (since 2026-11-16, when the
NMA's own loader was dissolved here; merge plan step 3).

The thresholds live in data_staging/cohort_constants.py and are re-exported
here for the analyses that print them.
"""

import os
import sys

import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_STAGING_DIR = os.path.normpath(os.path.join(_HERE, "..", "..", "data_staging"))


def analysis_dir():
    """The FDA_real_world_data/analysis/ directory, derived from this module's
    own location so it holds on Databricks too, where a file run from the Run
    button has no __file__ of its own."""
    return os.path.dirname(_HERE)

if _STAGING_DIR not in sys.path:
    sys.path.insert(0, _STAGING_DIR)

from cohort_constants import (  # noqa: E402,F401  (re-exported for the analyses)
    CARB_ENTRY_MAX_GRAMS,
    CARB_ENTRY_MIN_GRAMS,
    DIAGNOSIS_GROUPS,
    LADA_DIAGNOSIS_GROUPS,
    MAX_LOOP_VERSION_INT,
    MAX_SEG2_END_DATE,
    MIN_AGE_YEARS,
    MIN_CBG_COUNT,
    MIN_CGM_COVERAGE,
    PRIMARY_DIAGNOSIS_GROUPS,
    SAMPLES_PER_DAY,
    SEGMENT_DAYS,
    TYPE1_DX,
    diagnosis_group,
)

# Segment labels for the transition analysis tables
SEG1 = "tb_to_ab_seg1"  # temp basal period
SEG2 = "tb_to_ab_seg2"  # autobolus period

# A window is identified by its user and the first period's start date; an
# activation by its user and time.
SEGMENT_KEY = ["_userId", "tb_to_ab_seg1_start"]
ACTIVATION_KEY = ["_userId", "override_time"]

CATALOG = "dev.fda_510k_rwd"

# The cohort tables, by population, with the suffix placeholder for the
# window builds.
TRANSITION_COHORT_TABLE = "transition_cohort{suffix}"
ACTIVATION_COHORT_TABLE = "transition_activation_cohort{suffix}"
STABLE_COHORT_TABLE = "stable_cohort"
DURABILITY_COHORT_TABLE = "autobolus_durability{suffix}"

# Columns that never coerce to numeric.
_TEXT_COLUMNS = {
    "_userId", "segment", "gender", "diagnosis_group", "overridePreset", "dosing_mode",
    "override_time", "jaeb_pt_id", "tb_to_ab_max_loop_version", "adoption_date",
    "first_day", "last_day", "effective_last_day", "segment_start", "segment_end",
    "first_ab_day",
}


def _coerce_numeric(frame, keep=()):
    """Object columns that hold numbers (Spark decimals, numeric strings) to
    numeric; a column is left alone when any of its values would be lost,
    so labels, dates, timestamps, and identifiers keep their values.

    Spark dates and labels reach pandas as object columns; coercing them
    blindly turned every window's start date and every guardrail group into
    NaN (caught by the Databricks suite on 2026-11-06).
    """
    for column in frame.select_dtypes(include=["object"]).columns:
        if column in _TEXT_COLUMNS or column in keep or column.startswith(("is_", "has_", "in_", "passes_", "no_")):
            continue
        converted = pd.to_numeric(frame[column], errors="coerce")
        lost = converted.isna() & frame[column].notna()
        if lost.any():
            continue
        frame[column] = converted
    return frame


def _select_groups(frame, diagnosis_groups, label):
    """The rows of the diagnosis groups asked for; None keeps every group."""
    assert "diagnosis_group" in frame.columns, f"{label}: no diagnosis_group column"
    if diagnosis_groups is None:
        print(f"  {label}: every diagnosis group kept, {frame['_userId'].nunique()} users")
        return frame
    unknown = sorted(set(diagnosis_groups) - set(DIAGNOSIS_GROUPS))
    assert not unknown, f"unknown diagnosis group(s) {unknown}; known: {DIAGNOSIS_GROUPS}"
    kept = frame[frame["diagnosis_group"].isin(diagnosis_groups)].copy()
    print(f"  {label}: diagnosis groups {tuple(diagnosis_groups)} kept {kept['_userId'].nunique()} users")
    return kept


def as_bool(series):
    """A boolean Series from a staged flag column: True where the value is
    True (or 1), False for False, 0, None, and NaN. Spark booleans reach
    pandas as object columns when they hold NULLs."""
    return series.eq(True)


def _flags(frame, *columns):
    """Boolean Series of the named columns, a missing value read as False."""
    return [as_bool(frame[c]) for c in columns]


def _assert_equal_flags(frame, name, expected, label):
    staged = as_bool(frame[name])
    mismatches = int((staged != expected).sum())
    assert mismatches == 0, (
        f"{label}: the staged {name} disagrees with its criterion columns on {mismatches} row(s); "
        f"re-stage the cohort table before analysing"
    )


def _admit(frame, assert_predicates, predicate, diagnosis_groups, label, unit):
    """The pure half of every loader: coerce the numbers, assert the table's
    staged predicates against its criteria, keep the rows `predicate` admits
    (None keeps every row), and select the diagnosis groups asked for. Takes
    a pandas frame, so a loader's logic runs on a hand-built table without
    Spark."""
    frame = _coerce_numeric(frame)
    assert_predicates(frame, label=label)
    if predicate is not None:
        frame = frame[as_bool(frame[predicate])].copy()
        users = frame["_userId"].nunique()
        tail = "" if unit == "user(s)" else f" for {users} user(s)"
        print(f"  {label}: {predicate} admits {len(frame)} {unit}{tail}")
    return _select_groups(frame, diagnosis_groups, label)


# ---------------------------------------------------------------------------
# Predicate re-derivations: the loader's half of Gate C
# ---------------------------------------------------------------------------

def transition_predicates(frame):
    """Re-derive the transition cohort table's predicates from its criteria."""
    version, age, ep1, ep2, cov1, cov2, clean, carb1, carb2 = _flags(
        frame, "passes_loop_version_gate", "passes_age_gate", "has_endpoints_seg1",
        "has_endpoints_seg2", "passes_cgm_coverage_seg1", "passes_cgm_coverage_seg2",
        "no_guardrail_violation", "has_carb_data_seg1", "has_carb_data_seg2",
    )
    rank = pd.to_numeric(frame["segment_rank"], errors="coerce")
    passes_window = version & age & ep1 & ep2 & cov1 & cov2 & clean
    # The best window per user among those passing: the lowest rank, the
    # earliest start on a tie (the staging orders the same way).
    order = frame.assign(_passes=passes_window, _rank=rank).loc[passes_window]
    order = order.sort_values(["_userId", "_rank", "tb_to_ab_seg1_start"])
    best_index = order.groupby("_userId", sort=False).head(1).index
    is_best = pd.Series(frame.index.isin(best_index), index=frame.index)
    return pd.DataFrame({
        "passes_window_criteria": passes_window,
        "is_best_window": is_best,
        "in_transition_cohort": passes_window & is_best,
        "in_preset_cohort": version & age & clean & (rank == 1),
        "in_carb_cohort": passes_window & is_best & carb1 & carb2,
    })


def assert_transition_predicates(frame, label="transition cohort"):
    expected = transition_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} window(s)")


def activation_predicates(frame):
    window, p1, p2, p3, glucose, v2, v3 = _flags(
        frame, "window_in_preset_cohort", "is_in_period_seg1", "is_in_period_seg2",
        "is_in_period_seg3", "is_starting_glucose_in_range", "is_valid_name_only_seg2",
        "is_valid_name_only_seg3",
    )
    return pd.DataFrame({
        "in_activation_cohort": window,
        "in_duration_cohort": window & (p1 | p2),
        "in_paired_outcome_cohort_seg2": window & (p1 | p2) & glucose & v2,
        "in_paired_outcome_cohort_seg3": window & (p1 | p3) & glucose & v3,
    })


def assert_activation_predicates(frame, label="activation cohort"):
    expected = activation_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} activation(s)")


def stable_predicates(frame):
    endpoints, coverage, age, jaeb = _flags(
        frame, "has_endpoints", "passes_cgm_coverage", "passes_age_gate", "is_jaeb_linked",
    )
    return pd.DataFrame({
        "in_stable_cohort": endpoints & coverage & age,
        "in_jaeb_cohort": endpoints & coverage & age & jaeb,
    })


def assert_stable_predicates(frame, label="stable cohort"):
    expected = stable_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} segment(s)")


def durability_predicates(frame):
    adopted, followup, final, age, jaeb = _flags(
        frame, "is_adopted", "has_min_followup", "has_final_coverage", "is_age_eligible",
        "is_jaeb_linked",
    )
    return pd.DataFrame({
        "in_durability_cohort": adopted & followup & final & age,
        "in_jaeb_cohort": adopted & followup & final & age & jaeb,
    })


def assert_durability_predicates(frame, label="durability cohort"):
    expected = durability_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} user(s)")


# ---------------------------------------------------------------------------
# The transition population (6-3a, 8-1, 8-5, 8-8; the segment cohort of
# 8-2, 8-3, 8-4, IR-1, IR-6, IR-6B)
# ---------------------------------------------------------------------------

def load_transition_cohort(spark, suffix="", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                           predicate="in_transition_cohort"):
    """The windows a predicate admits, as pandas, every column of the cohort
    table. `predicate` is one of the table's predicate columns
    (in_transition_cohort, in_preset_cohort, in_carb_cohort, or None for
    every window). The whole table is read first so the best-window rule can
    be re-derived across each user's windows."""
    table = f"{CATALOG}.{TRANSITION_COHORT_TABLE.format(suffix=suffix)}"
    return prepare_transition_cohort(spark.table(table).toPandas(), predicate, diagnosis_groups, label=table)


def prepare_transition_cohort(frame, predicate="in_transition_cohort", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                              label="transition cohort"):
    """The pure half of load_transition_cohort, on the cohort table as a frame."""
    return _admit(frame, assert_transition_predicates, predicate, diagnosis_groups, label, "window(s)")


def load_transition_endpoints(spark, suffix="", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS):
    """Glycemic endpoints of the transition cohort, one row per user, the two
    periods side by side (`_seg1` and `_seg2` suffixes), with the window's
    rank, demographics, and diagnosis group from the cohort table.

    The cohort is `transition_cohort{suffix}.in_transition_cohort`, the
    diagnosis groups asked for (the primary cohort by default, None for every
    group).
    """
    cohort = load_transition_cohort(spark, suffix=suffix, diagnosis_groups=diagnosis_groups)
    endpoints_table = f"{CATALOG}.glycemic_endpoints_transition{suffix}"
    endpoints = spark.table(endpoints_table).toPandas()
    endpoints = _coerce_numeric(endpoints)
    keys = cohort[SEGMENT_KEY].drop_duplicates()
    endpoints = endpoints.merge(keys, on=SEGMENT_KEY, how="inner")

    seg1 = endpoints[endpoints["segment"] == SEG1].set_index(SEGMENT_KEY).add_suffix("_seg1")
    seg2 = endpoints[endpoints["segment"] == SEG2].set_index(SEGMENT_KEY).add_suffix("_seg2")
    wide = seg1.join(seg2, how="inner").reset_index()
    wide = wide.drop(columns=["segment_seg1", "segment_seg2"], errors="ignore")
    assert len(wide) == len(keys), (
        f"{endpoints_table}: {len(keys)} cohort window(s) but {len(wide)} with both periods' "
        f"endpoints; the cohort table and the endpoints table are out of step"
    )

    window_columns = SEGMENT_KEY + [
        "tb_to_ab_seg2_end", "segment_rank", "tb_to_ab_segment_score",
        "tb_to_ab_max_loop_version", "tb_to_ab_max_loop_version_int",
        "tb_to_ab_age_years", "tb_to_ab_years_lwd", "gender",
        "diagnosis_group", "is_primary_diagnosis",
        "has_carb_data_seg1", "has_carb_data_seg2", "in_carb_cohort",
    ]
    wide = wide.merge(cohort[window_columns], on=SEGMENT_KEY, how="inner")
    assert wide["_userId"].is_unique, "the transition cohort is not one window per user"
    print(f"  Transition cohort: {len(wide)} user(s) with both periods' endpoints")
    return wide


def load_allowed_transition_segments(spark, suffix="", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS):
    """The rank-1 windows of the preset population (`in_preset_cohort`), as
    a Spark DataFrame of (`_userId`, `tb_to_ab_seg1_start`) for the analyses
    that join activations in Spark (IR-6, IR-6B)."""
    cohort = load_transition_cohort(spark, suffix=suffix, diagnosis_groups=diagnosis_groups,
                                    predicate="in_preset_cohort")
    # An explicit schema: an empty selection (no window in the groups asked
    # for) has no rows to infer one from.
    return spark.createDataFrame(cohort[SEGMENT_KEY], schema="_userId STRING, tb_to_ab_seg1_start DATE")


# ---------------------------------------------------------------------------
# The preset-activation population (8-2, 8-3, 8-4, IR-1)
# ---------------------------------------------------------------------------

def load_activation_cohort(spark, suffix="", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                           predicate="in_activation_cohort"):
    """The activations a predicate admits, with every column of
    overrides_by_segment joined back on the activation key.

    `predicate` is one of in_activation_cohort (IR-1), in_duration_cohort
    (8-4), in_paired_outcome_cohort_seg2 (8-2 Table 8.2b, 8-3), or
    in_paired_outcome_cohort_seg3 (8-2 Table 8.2c); None keeps every
    activation of a rank-1 window.
    """
    table = f"{CATALOG}.{ACTIVATION_COHORT_TABLE.format(suffix=suffix)}"
    flags = prepare_activation_cohort(spark.table(table).toPandas(), predicate, diagnosis_groups, label=table)

    overrides = spark.table(f"{CATALOG}.overrides_by_segment{suffix}").toPandas()
    overrides = overrides.drop(columns=[c for c in overrides.columns
                                        if c in flags.columns and c not in ACTIVATION_KEY])
    merged = flags.merge(overrides, on=ACTIVATION_KEY, how="inner")
    assert len(merged) == len(flags), (
        f"{table}: {len(flags)} activation(s) but {len(merged)} found in overrides_by_segment{suffix}"
    )
    return _coerce_numeric(merged)


def prepare_activation_cohort(frame, predicate="in_activation_cohort", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                              label="activation cohort"):
    """The pure half of load_activation_cohort, on the cohort table as a frame."""
    return _admit(frame, assert_activation_predicates, predicate, diagnosis_groups, label, "activation(s)")


def prepare_override_activations(frame, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, label="activation cohort"):
    """The pure half of load_override_endpoints: the activations of the preset
    cohort's windows with a starting glucose in range, any period."""
    frame = _coerce_numeric(frame)
    assert_activation_predicates(frame, label=label)
    window, glucose = _flags(frame, "window_in_preset_cohort", "is_starting_glucose_in_range")
    frame = frame[window & glucose].copy()
    print(f"  {label}: window in the preset cohort and starting glucose in range admits "
          f"{len(frame)} activation(s) for {frame['_userId'].nunique()} user(s)")
    return _select_groups(frame, diagnosis_groups, label)


def load_override_endpoints(spark, suffix="", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS):
    """Per-activation glycemic endpoints for analysis 8-2: the activations of
    the preset cohort's windows with a starting glucose in range, any period,
    joined to glycemic_endpoints_override on the activation key. The
    name-only validity and the period pairing are applied by
    `aggregate_override_endpoints`, since the second and third periods have
    their own validity flags.

    Returns one row per activation with `window_hours` (the preset duration
    plus a 2-hour tail).
    """
    table = f"{CATALOG}.{ACTIVATION_COHORT_TABLE.format(suffix=suffix)}"
    flags = prepare_override_activations(spark.table(table).toPandas(), diagnosis_groups, label=table)

    endpoints = spark.table(f"{CATALOG}.glycemic_endpoints_override{suffix}").toPandas()
    endpoints = endpoints.drop(columns=[c for c in endpoints.columns
                                        if c in flags.columns and c not in ACTIVATION_KEY])
    merged = flags.merge(endpoints, on=ACTIVATION_KEY, how="inner")
    merged = _coerce_numeric(merged)
    print(f"  {len(merged)} activation(s) carry glycemic endpoints")

    # Per-activation exposure window (preset duration + 2-hour tail), in
    # hours. Duration is in seconds.
    merged["window_hours"] = (pd.to_numeric(merged["duration"], errors="coerce") + 7200) / 3600.0
    return merged


_ENDPOINT_AVG_COLS = (
    "tbr_very_low", "tbr", "tir", "tar", "tar_very_high",
    "mean_glucose", "cv",
)
_GRAIN_COLS = {
    "name":   ["overridePreset"],
    "config": ["overridePreset", "brsf", "btl", "bth", "crsf", "issf"],
}


def aggregate_override_endpoints(
    activations: pd.DataFrame,
    ab_segment: str,
    grain: str,
) -> pd.DataFrame:
    """
    Average per-activation endpoints up to the requested grain and return
    a wide DataFrame pairing temp_basal with the requested AB segment.

    Parameters
    ----------
    activations : pd.DataFrame
        Per-activation rows from `load_override_endpoints`.
    ab_segment : {"tb_to_ab_seg2", "tb_to_ab_seg3"}
        Which AB segment to pair with TB. Selects the matching
        `is_valid_name_only_*` validity column.
    grain : {"name", "config"}
        "name"   → (user, overridePreset)
        "config" → (user, overridePreset, brsf, btl, bth, crsf, issf)

    Aggregation rules (per the analysis plan):
    - Range and shape endpoints (TIR / TBR / TAR / mean / CV): unweighted
      mean across activations within the group.
    - Hypo events: total events ÷ total exposure hours → events/hour.
    - activation_count: number of activations contributing to the group.

    The returned DataFrame has one row per (user, grain_key) with `_seg1`
    (TB) and `_seg2` (the requested AB segment) suffixed columns; `_seg2`
    is used in the suffix regardless of which AB segment was requested so
    downstream code (paired stats, plotting) doesn't need to know.
    """
    if grain not in _GRAIN_COLS:
        raise ValueError(f"grain must be 'name' or 'config', got {grain!r}")
    if ab_segment not in ("tb_to_ab_seg2", "tb_to_ab_seg3"):
        raise ValueError(f"ab_segment must be 'tb_to_ab_seg2' or 'tb_to_ab_seg3', got {ab_segment!r}")

    valid_col = "is_valid_name_only_seg2" if ab_segment == "tb_to_ab_seg2" else "is_valid_name_only_seg3"
    df = activations[activations[valid_col] == True].copy()  # noqa: E712
    df = df[df["segment"].isin(["tb_to_ab_seg1", ab_segment])].copy()

    grain_cols = _GRAIN_COLS[grain]
    group_cols = ["_userId"] + grain_cols + ["segment"]

    agg_spec = {col: "mean" for col in _ENDPOINT_AVG_COLS}
    agg_spec["hypo_events"] = "sum"
    agg_spec["window_hours"] = "sum"

    aggregated = df.groupby(group_cols, as_index=False).agg(agg_spec)
    sizes = (
        df.groupby(group_cols, as_index=False)
        .size()
        .rename(columns={"size": "activation_count"})
    )
    aggregated = aggregated.merge(sizes, on=group_cols, how="left")
    # Hypo rate per hour of preset exposure (window = duration + 2h tail).
    aggregated["hypo_rate"] = (
        aggregated["hypo_events"] / aggregated["window_hours"]
    ).where(aggregated["window_hours"] > 0)

    # Pivot to wide on segment.
    index_cols = ["_userId"] + grain_cols
    seg1 = (aggregated[aggregated["segment"] == "tb_to_ab_seg1"]
            .set_index(index_cols).drop(columns=["segment"]).add_suffix("_seg1"))
    seg2 = (aggregated[aggregated["segment"] == ab_segment]
            .set_index(index_cols).drop(columns=["segment"]).add_suffix("_seg2"))
    wide = seg1.join(seg2, how="inner").reset_index()
    return wide


# ---------------------------------------------------------------------------
# The stable-autobolus population (8-6) and the durability population (8-7)
# ---------------------------------------------------------------------------

def load_stable_cohort(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, predicate="in_stable_cohort"):
    """The stable segments a predicate admits (in_stable_cohort, or
    in_jaeb_cohort for the socioeconomic hand-off), one row per user, with
    the segment's glycemic endpoints joined on."""
    table = f"{CATALOG}.{STABLE_COHORT_TABLE}"
    frame = prepare_stable_cohort(spark.table(table).toPandas(), predicate, diagnosis_groups, label=table)

    endpoints = spark.table(f"{CATALOG}.glycemic_endpoints_stable_autobolus").toPandas()
    endpoints = _coerce_numeric(endpoints).drop(columns=["segment", "cbg_count"], errors="ignore")
    merged = frame.merge(endpoints, on="_userId", how="left")
    assert merged["_userId"].is_unique, "the stable cohort is not one segment per user"
    return merged


def prepare_stable_cohort(frame, predicate="in_stable_cohort", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                          label="stable cohort"):
    """The pure half of load_stable_cohort, before the endpoints join."""
    return _admit(frame, assert_stable_predicates, predicate, diagnosis_groups, label, "user(s)")


def prepare_durability_cohort(frame, predicate="in_durability_cohort", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                              label="durability cohort"):
    """The pure half of load_durability_cohort."""
    return _admit(frame, assert_durability_predicates, predicate, diagnosis_groups, label, "user(s)")


def load_durability_cohort(spark, table_suffix="", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                           predicate="in_durability_cohort"):
    """The users a predicate admits from autobolus_durability{table_suffix}
    (in_durability_cohort for Table 8.7a and the curves, in_jaeb_cohort for
    the hand-off), with every column of the table."""
    table = f"{CATALOG}.{DURABILITY_COHORT_TABLE.format(suffix=table_suffix)}"
    return prepare_durability_cohort(spark.table(table).toPandas(), predicate, diagnosis_groups, label=table)


# ---------------------------------------------------------------------------
# The interactive-review populations (2026-11-05): the autobolus-day
# population of IR-1002, the extreme-preset population of IR-6, and the
# dose-response episodes of IR-6B
# ---------------------------------------------------------------------------

AB_DAY_COHORT_TABLE = "ab_day_cohort"
GUARDRAIL_GROUPS_TABLE = "user_guardrail_groups"
GUARDRAIL_FLAGS_TABLE = "override_guardrail_flags"
EXTREME_PRESET_COHORT_TABLE = "extreme_preset_cohort"
EXTREME_PRESET_ACTIVATIONS_TABLE = "extreme_preset_activations"
DOSE_RESPONSE_CANDIDATES_TABLE = "dose_response_candidates"
DOSE_RESPONSE_EPISODES_TABLE = "dose_response_episodes"

# The columns of the day table a flow or a count needs; the table is one
# row per user-day, so the loaders read these unless asked for more.
AB_DAY_FLAG_COLUMNS = [
    "_userId", "day", "diagnosis_group", "is_primary_diagnosis", "is_ab_day",
    "is_version_eligible", "is_age_eligible", "cbg_day_count", "is_coverage_ok",
    "is_eligible_ab_day", "is_outcome_day", "first_eligible_ab_day",
]


def ab_day_predicates(frame):
    ab, version, age, coverage = _flags(frame, "is_ab_day", "is_version_eligible",
                                        "is_age_eligible", "is_coverage_ok")
    eligible = ab & version & age
    return pd.DataFrame({"is_eligible_ab_day": eligible, "is_outcome_day": eligible & coverage})


def assert_ab_day_predicates(frame, label="autobolus-day cohort"):
    expected = ab_day_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} user-day(s)")


def load_ab_day_cohort(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, predicate="is_outcome_day",
                       columns=AB_DAY_FLAG_COLUMNS):
    """The user-days a predicate admits from ab_day_cohort (is_outcome_day
    for the IR-2 outcome days, is_eligible_ab_day for the IR-3 rate basis,
    None for every day), the flag columns by default."""
    table = f"{CATALOG}.{AB_DAY_COHORT_TABLE}"
    frame = spark.table(table)
    if columns is not None:
        frame = frame.select(*columns)
    return prepare_ab_day_cohort(frame.toPandas(), predicate, diagnosis_groups, label=table)


def prepare_ab_day_cohort(frame, predicate="is_outcome_day", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                          label="autobolus-day cohort"):
    """The pure half of load_ab_day_cohort."""
    return _admit(frame, assert_ab_day_predicates, predicate, diagnosis_groups, label, "user-day(s)")


GUARDRAIL_GROUP_LABELS = ("never_preset", "compliant", "p_only", "m_only", "both")


def guardrail_group_predicates(frame):
    """The five-group label re-derived from the counts and flags it summarises."""
    ever_p, ever_m = _flags(frame, "ever_p", "ever_m")
    qualifying = pd.to_numeric(frame["n_qualifying_activations"], errors="coerce").fillna(0)
    label = pd.Series("both", index=frame.index)
    label[ever_p & ~ever_m] = "p_only"
    label[~ever_p & ever_m] = "m_only"
    label[~ever_p & ~ever_m] = "compliant"
    label[qualifying == 0] = "never_preset"
    return pd.DataFrame({"guardrail_group": label})


def assert_guardrail_group_predicates(frame, label="guardrail groups"):
    expected = guardrail_group_predicates(frame)["guardrail_group"]
    mismatches = int((frame["guardrail_group"].astype(str) != expected).sum())
    assert mismatches == 0, f"{label}: guardrail_group disagrees with its flags on {mismatches} user(s)"
    unknown = sorted(set(frame["guardrail_group"].astype(str)) - set(GUARDRAIL_GROUP_LABELS))
    assert not unknown, f"{label}: unknown guardrail group(s) {unknown}"
    print(f"  {label}: guardrail groups reproduced on {len(frame)} user(s)")


def load_guardrail_group_cohort(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, predicate="in_outcome_cohort"):
    """The users a predicate admits from user_guardrail_groups: every user
    with an eligible autobolus day (None), or those with an outcome day
    (in_outcome_cohort, the IR-2 population), with their group."""
    table = f"{CATALOG}.{GUARDRAIL_GROUPS_TABLE}"
    return prepare_guardrail_group_cohort(spark.table(table).toPandas(), predicate, diagnosis_groups, label=table)


def prepare_guardrail_group_cohort(frame, predicate="in_outcome_cohort", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                                   label="guardrail groups"):
    """The pure half of load_guardrail_group_cohort."""
    return _admit(frame, assert_guardrail_group_predicates, predicate, diagnosis_groups, label, "user(s)")


def guardrail_flag_predicates(frame):
    version, after, all_days = _flags(frame, "is_version_eligible", "is_after_first_ab", "is_all_days_ab")
    return pd.DataFrame({
        "is_qualifying": version & after,
        "in_characterization_cohort": version & after & all_days,
    })


def assert_guardrail_flag_predicates(frame, label="guardrail flags"):
    expected = guardrail_flag_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} activation(s)")


def load_guardrail_flags(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, predicate="is_qualifying"):
    """The activations a predicate admits from override_guardrail_flags
    (is_qualifying for IR-2's checks, in_characterization_cohort for IR-3,
    None for every activation), with the P and M verdicts."""
    table = f"{CATALOG}.{GUARDRAIL_FLAGS_TABLE}"
    return prepare_guardrail_flags(spark.table(table).toPandas(), predicate, diagnosis_groups, label=table)


def prepare_guardrail_flags(frame, predicate="is_qualifying", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                            label="guardrail flags"):
    """The pure half of load_guardrail_flags."""
    return _admit(frame, assert_guardrail_flag_predicates, predicate, diagnosis_groups, label, "activation(s)")


def extreme_preset_predicates(frame):
    """The IR-6 user-segment predicate: an extreme-preset member with CGM
    that reaches the coverage floor in the segment."""
    member, cgm, coverage = _flags(frame, "any_extreme", "has_cgm", "passes_cgm_coverage")
    return pd.DataFrame({"in_extreme_cohort": member & cgm & coverage})


def assert_extreme_preset_predicates(frame, label="extreme-preset cohort"):
    expected = extreme_preset_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} user-segment(s)")


def load_extreme_preset_cohort(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, predicate="in_extreme_cohort"):
    """The user-segments a predicate admits from extreme_preset_cohort (IR-6):
    the category memberships, the per-segment active flags and exposure, and
    the whole-segment endpoints."""
    table = f"{CATALOG}.{EXTREME_PRESET_COHORT_TABLE}"
    return prepare_extreme_preset_cohort(spark.table(table).toPandas(), predicate, diagnosis_groups, label=table)


def prepare_extreme_preset_cohort(frame, predicate="in_extreme_cohort", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                                  label="extreme-preset cohort"):
    """The pure half of load_extreme_preset_cohort."""
    return _admit(frame, assert_extreme_preset_predicates, predicate, diagnosis_groups, label, "user-segment(s)")


def load_extreme_preset_activations(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS):
    """Every preset activation of the preset cohort's rank-1 windows with
    its extreme-category flags (IR-6's Table IR-6b and exposure)."""
    table = f"{CATALOG}.{EXTREME_PRESET_ACTIVATIONS_TABLE}"
    frame = _coerce_numeric(spark.table(table).toPandas())
    return _select_groups(frame, diagnosis_groups, table)


def dose_response_candidate_predicates(frame):
    c1, c2, tb, ab = _flags(frame, "in_c1", "in_c2", "is_phase_tb", "is_phase_ab")
    return pd.DataFrame({
        "in_candidate_pool": c1 | c2,
        "in_series_C1a": c1 & tb,
        "in_series_C1b": c1 & ab,
        "in_series_C2": c2,
    })


def assert_dose_response_candidate_predicates(frame, label="dose-response candidates"):
    expected = dose_response_candidate_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} activation(s)")


def dose_response_episode_predicates(frame):
    single, isolated, duration, start, coverage = _flags(
        frame, "pass_single_day", "pass_isolation", "pass_duration", "pass_start_bg", "pass_coverage",
    )
    survivor = single & isolated & duration
    return pd.DataFrame({"is_survivor": survivor, "is_retained": survivor & start & coverage})


def assert_dose_response_episode_predicates(frame, label="dose-response episodes"):
    expected = dose_response_episode_predicates(frame)
    for name in expected.columns:
        _assert_equal_flags(frame, name, expected[name], label)
    print(f"  {label}: predicates reproduced on {len(frame)} episode(s)")


# The timestamp columns of the episode tables travel as strings, as the
# analysis built them before 2026-11-05 (Spark's CAST(timestamp AS STRING)
# writes fractional seconds only when present).
_DOSE_RESPONSE_TIMESTAMP_COLUMNS = ("t0", "window_start", "window_end")


def _dose_response_frame(spark, table):
    rows = spark.table(table)
    for column in _DOSE_RESPONSE_TIMESTAMP_COLUMNS + ("override_day",):
        if column in rows.columns:
            rows = rows.withColumn(column, rows[column].cast("string"))
    return _coerce_numeric(rows.toPandas())


def load_dose_response_candidates(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, predicate="in_candidate_pool"):
    """Every preset activation with its grain memberships and filters 3 to 5
    (dose_response_candidates, IR-6B); the candidate pool by default."""
    table = f"{CATALOG}.{DOSE_RESPONSE_CANDIDATES_TABLE}"
    return prepare_dose_response_candidates(_dose_response_frame(spark, table), predicate, diagnosis_groups, label=table)


def prepare_dose_response_candidates(frame, predicate="in_candidate_pool", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                                     label="dose-response candidates"):
    """The pure half of load_dose_response_candidates."""
    return _admit(frame, assert_dose_response_candidate_predicates, predicate, diagnosis_groups, label, "activation(s)")


def load_dose_response_episodes(spark, diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS, predicate="is_survivor"):
    """The episodes a predicate admits from dose_response_episodes (IR-6B):
    the filter-5 survivors by default, with their window CGM counts, the
    starting glucose, the hypo events, and filters 6 and 7 as columns."""
    table = f"{CATALOG}.{DOSE_RESPONSE_EPISODES_TABLE}"
    return prepare_dose_response_episodes(_dose_response_frame(spark, table), predicate, diagnosis_groups, label=table)


def prepare_dose_response_episodes(frame, predicate="is_survivor", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS,
                                   label="dose-response episodes"):
    """The pure half of load_dose_response_episodes."""
    return _admit(frame, assert_dose_response_episode_predicates, predicate, diagnosis_groups, label, "episode(s)")


# ---------------------------------------------------------------------------
# The PLN-1008 no-meal-announcement population (the NMA analyses 7-1, 8-1 to
# 8-4, and 8-supp): one row per user-day of nma_user_day_analysis_ready, the
# cohort table and the terminal table of the NMA staging chain (decision
# D23, 2026-11-05). The analyses run on Databricks through load_nma_day_level
# or locally off the CSV snapshot at nma_snapshot_csv(); either way the
# frame goes through prepare_nma_day_level, then select_nma_cohort for the
# §7.6 age split and the diagnosis groups, then restrict_nma_comparator for
# the §8.1 comparator rule. The thresholds are cohort_constants.py's.
# ---------------------------------------------------------------------------

NMA_ANALYSIS_READY_TABLE = "nma_user_day_analysis_ready"

# The cohort table's criterion columns, and the predicates the loader re-derives from them.
NMA_CRITERION_COLUMNS = ("passes_loop_version_gate", "day_eligible", "user_eligible", "passes_age_gate")
NMA_PREDICATE_COLUMNS = ("in_analysis_cohort", "in_strategy_contrast")

# (column, display label) — PLN-1008 §6 endpoints in the endpoints table.
NMA_ENDPOINTS = [
    ("tir", "Time 70-180 mg/dL (%)"),
    ("tbr", "Time <70 mg/dL (%)"),
    ("tbr_very_low", "Time <54 mg/dL (%)"),
    ("tar", "Time >180 mg/dL (%)"),
    ("tar_very_high", "Time >250 mg/dL (%)"),
    ("mean_glucose", "Mean glucose (mg/dL)"),
    ("cv", "CV (%)"),
    ("hypo_events", "Hypo events / day"),
]

# The three nested NMA-like classifications: (arm flag column, label).
CLASSIFICATIONS = [
    ("in_ce0_be0", "CE=0/BE=0"),
    ("in_ce0_be_le1", "CE=0/BE<=1"),
    ("in_ce0_be_inf", "CE=0/BE<=inf"),
]
COMPARATOR_FLAG = "in_ce_gt0"
COMPARATOR_LABEL = "CE>0"

# Arms shown in figures (3 nested NMA arms + comparator).
FIGURE_ARMS = CLASSIFICATIONS + [(COMPARATOR_FLAG, COMPARATOR_LABEL)]

# High meal-announcement ("high engagement") arm: >=3 carb entries AND >=3 manual bolus entries —
# the heavy-announcement end of the spectrum, shown as a 5th category beside the 3 NMA arms + CE>0
# (Table 8.1a column + figures 8.1a/8.1b) and in parallel supplement contrast tables. Staged as
# `in_ce_ge3_be_ge3` in export_nma_user_day_classification.py (carried through analysis-ready).
HIGH_MA_FLAG = "in_ce_ge3_be_ge3"
HIGH_MA_LABEL = "CE>=3/BE>=3"
# The 5-category arm set for the supplement / violins (3 NMA + CE>0 + high meal-announcement).
SUPPLEMENT_ARMS = FIGURE_ARMS + [(HIGH_MA_FLAG, HIGH_MA_LABEL)]

# §7.3 delivery strategies: (column value, short label), in DISPLAY order (TB before AB, per MJC) —
# this drives the §8.2 figure cell/line/x-axis order and the table row order. The LMM reference is
# AB (autobolus_on), the first level alphabetically (treatment coding) whatever this list's order, so coefficients
# are unaffected by the ordering. Any other / null strategy is "ambiguous" and excluded (the staging
# CASE currently emits only these two values). Shared by §8.2 + §8.3 (cross-tab) — single source.
DELIVERY_STRATEGIES = [("temp_basal_only", "TB"), ("autobolus_on", "AB")]
DELIVERY_STRATEGY_COLUMN = "delivery_strategy"

# Rolling temporal-match window for the §8.1 windowed-comparator sensitivity: a CE=0 (NMA) day is
# paired only against CE>0 comparator days within ± NMA_WINDOW_HALF calendar days of it (same user), to
# control within-user temporal drift (Loop-version era, seasonality). See windowed_matched_means.
NMA_WINDOW_DAYS = 90
NMA_WINDOW_HALF = 45


def nma_snapshot_csv(analysis_ready_table=NMA_ANALYSIS_READY_TABLE):
    """The snapshot data_staging/export_nma_user_day_analysis_ready.py writes, under
    FDA_real_world_data/outputs/nma/: <table-name>.csv.gz since 2026-11-17 (gzipped, which
    pandas reads as it reads a plain CSV), or the plain <table-name>.csv of an earlier
    vintage when only that is on disk. With neither, the gzipped path, where the next
    download goes. Takes the bare or the catalog-qualified table name."""
    stem = os.path.normpath(os.path.join(analysis_dir(), "..", "outputs", "nma", analysis_ready_table.split(".")[-1]))
    for path in (stem + ".csv.gz", stem + ".csv"):
        if os.path.exists(path):
            return path
    return stem + ".csv.gz"


def nma_day_predicates(frame):
    """Re-derive the NMA cohort table's predicates from its criterion columns."""
    version, day, user, age = _flags(frame, *NMA_CRITERION_COLUMNS)
    in_cohort = version & day & user & age
    return pd.DataFrame({
        "in_analysis_cohort": in_cohort,
        "in_strategy_contrast": in_cohort & frame[DELIVERY_STRATEGY_COLUMN].notna(),
    })


def assert_nma_day_predicates(frame, label=NMA_ANALYSIS_READY_TABLE):
    """Refuse a cohort table whose staged predicates disagree with its criteria (the
    loader's half of execution plan Gate C), or that predates the migration."""
    missing = [c for c in NMA_CRITERION_COLUMNS + NMA_PREDICATE_COLUMNS + ("diagnosis_group",)
               if c not in frame.columns]
    assert not missing, (
        f"{label}: no column(s) {missing}; the table predates the 2026-11-05 cohort-table "
        f"migration (D23). Re-run data_staging/export_nma_user_day_analysis_ready.py."
    )
    expected = nma_day_predicates(frame)
    for name in NMA_PREDICATE_COLUMNS:
        _assert_equal_flags(frame, name, expected[name], label)


def prepare_nma_day_level(frame, label=NMA_ANALYSIS_READY_TABLE):
    """The pure half of load_nma_day_level: coerce the numbers, assert the cohort table's
    predicates, and keep the rows of the analysis cohort (`in_analysis_cohort`: the version
    gate, CGM coverage, the user's eligible-day count, the age gate; §7.1 and §6). Every
    diagnosis group is kept; select_nma_cohort selects."""
    return _admit(frame, assert_nma_day_predicates, "in_analysis_cohort", None, label, "user-day(s)")


# The prepared frame of each table version already pulled in this process. The five analyses
# run three cohorts each and every cohort run asks for the same rows, so without it one
# driver run pulled the whole cohort table from Spark fifteen times (2026-11-17).
_NMA_DAY_LEVEL_PULLS = {}


def _delta_version(spark, table):
    """The table's latest Delta version, or None when it cannot be read (a test double, a
    table that is not Delta). A restaged table has a new version, so it is pulled again."""
    try:
        return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"]
    except Exception:  # noqa: BLE001  (no history means no reuse, never a failed load)
        return None


def forget_nma_day_level():
    """Drop every kept pull (the tests, and a caller that wants a fresh read)."""
    _NMA_DAY_LEVEL_PULLS.clear()


def load_nma_day_level(spark, analysis_ready_table=f"{CATALOG}.{NMA_ANALYSIS_READY_TABLE}"):
    """The analysis cohort's rows, with arm flags + endpoints. One pull per table version per
    process: a second call for the same version returns the same frame, which callers must not
    change in place (select_nma_cohort, the step every analysis takes next, returns a copy)."""
    version = _delta_version(spark, analysis_ready_table)
    key = (analysis_ready_table, version)
    if version is not None and key in _NMA_DAY_LEVEL_PULLS:
        print(f"  {analysis_ready_table}: reusing the pull of version {version} made earlier in this run")
        return _NMA_DAY_LEVEL_PULLS[key]
    frame = prepare_nma_day_level(spark.table(analysis_ready_table).toPandas(), label=analysis_ready_table)
    if version is not None:
        _NMA_DAY_LEVEL_PULLS.clear()          # one table version at a time: an older pull is of no use
        _NMA_DAY_LEVEL_PULLS[key] = frame
    return frame


def select_nma_cohort(frame, cohort="all", diagnosis_groups=PRIMARY_DIAGNOSIS_GROUPS):
    """§7.6 age cohort and the diagnosis-group selection. NULL `is_pediatric` (DOB unknown or
    implausible) is excluded from both adult and pediatric, retained in `all`. The §6 age
    floor is a column of the cohort table (`passes_age_gate`) and already applied by
    prepare_nma_day_level.

    `diagnosis_groups` selects the groups kept (cohort_constants.DIAGNOSIS_GROUPS): the
    primary cohort by default (type 1, the LADA-labelled JAEB members included), None for
    every group (the by-group appendix). The group is a column, never a filter at staging."""
    out = frame
    if cohort == "adult":
        out = out[out["is_pediatric"] == False]  # noqa: E712
    elif cohort == "pediatric":
        out = out[out["is_pediatric"] == True]  # noqa: E712
    elif cohort != "all":
        raise ValueError(f"unknown cohort: {cohort!r}")
    return _select_groups(out, diagnosis_groups, f"NMA cohort {cohort!r}").copy()


def restrict_nma_comparator(frame):
    """§8.1 Table 8.1a: the comparison arms — CE>0 and the CE>=3/BE>=3 high meal-announcement
    supplement arm — are restricted to users who contributed >=1 CE=0 day in at least one of the
    three NMA classifications. The arms are nested under in_ce0_be_inf (CE=0), so that reduces to
    ">=1 in_ce0_be_inf day". Zero in_ce_gt0 (and in_ce_ge3_be_ge3) for users with no CE=0 day so
    every table/figure compares the same CE=0-contributing cohort."""
    ce0_users = set(frame.loc[frame["in_ce0_be_inf"] == True, "_userId"])  # noqa: E712
    out = frame.copy()
    non_ce0 = ~out["_userId"].isin(ce0_users)
    out.loc[non_ce0, "in_ce_gt0"] = False
    if HIGH_MA_FLAG in out.columns:
        out.loc[non_ce0, HIGH_MA_FLAG] = False
    return out


def windowed_matched_means(pdf, nma_flag, cmp_flag, endpoints, half=NMA_WINDOW_HALF):
    """Per-NMA-day ±`half`-day matched comparator inputs (the §8.1 windowed-comparator sensitivity).

    For the §8.1 NMA-vs-CE>0 contrast, anchor on each NMA (`nma_flag`) day; its comparator value is
    the MEAN of `cmp_flag` comparator days (CE>0 = COMPARATOR_FLAG) within ±half CALENDAR days of
    THAT day (same user). Only NMA days with >=1 in-window comparator are kept; a comparator day
    near several NMA days contributes to each of their local means. Matching contemporaneous days
    removes within-user temporal drift (Loop-version era, seasonality) from the contrast.

    Returns (per_user, coverage):
      per_user — one row per user with >=1 matched NMA day; columns `{ep}__nma` (mean over the
                 user's matched NMA days of the NMA-day value) and `{ep}__cmp` (mean over those
                 days of the local comparator mean) for each ep in `endpoints`. The per-user
                 windowed contrast for `ep` is `{ep}__nma - {ep}__cmp` (equal-user weight, D5).
      coverage — dict(total_nma_days, matched_nma_days, n_users_matched).

    Requires `nma_flag` + `cmp_flag` boolean columns and `local_day` (YYYY-MM-DD). Implementation:
    per user, encode days as calendar-day ordinals and use cumulative-sum + searchsorted so each
    NMA day's in-window comparator mean is O(log n)."""
    ep_cols = [e[0] if isinstance(e, (tuple, list)) else e for e in endpoints]
    ordv_all = (pd.to_datetime(pdf["local_day"]) - pd.Timestamp("2000-01-01")).dt.days
    rows = []
    total_nma = matched_nma = 0
    for _, sub in pdf.groupby("_userId"):
        nm = (sub[nma_flag] == True).to_numpy()   # noqa: E712
        # Comparator pool excludes the treatment arm's own days: no-op for disjoint CE=0-vs-CE>0 matching;
        # for the overlapping CE>=3/BE>=3 windowed contrast it stops HMA days being their own comparator.
        cm = ((sub[cmp_flag] == True) & (sub[nma_flag] == False)).to_numpy()    # noqa: E712
        n_nma = int(nm.sum())
        if n_nma == 0:
            continue
        total_nma += n_nma
        if cm.sum() == 0:
            continue
        ordv = ordv_all.loc[sub.index].to_numpy()
        n_pos = np.where(nm)[0]
        c_pos = np.where(cm)[0]
        n_ord = ordv[n_pos]
        c_ord = ordv[c_pos]
        order = np.argsort(c_ord)
        c_ord_s = c_ord[order]
        lo = np.searchsorted(c_ord_s, n_ord - half, side="left")
        hi = np.searchsorted(c_ord_s, n_ord + half, side="right")   # inclusive ±half (91-day window)
        cnt = hi - lo
        keep = cnt > 0
        matched_nma += int(keep.sum())
        if not keep.any():
            continue
        rec = {"_userId": sub["_userId"].iloc[0]}
        for ep in ep_cols:
            cv = sub[ep].to_numpy()[c_pos][order].astype(float)
            prefix = np.concatenate([[0.0], np.cumsum(cv)])         # prefix sums over sorted comparator days
            local = (prefix[hi] - prefix[lo]) / np.where(cnt > 0, cnt, 1)   # local comparator mean per NMA day
            nv = sub[ep].to_numpy()[n_pos].astype(float)
            rec[f"{ep}__nma"] = np.nanmean(nv[keep])
            rec[f"{ep}__cmp"] = np.nanmean(local[keep])
        rows.append(rec)
    per_user = pd.DataFrame(rows)
    if per_user.empty:
        # No user had a matched NMA day (e.g. an arm with no qualifying days — like the HMA arm in a
        # cohort/fixture with no CE>=3/BE>=3 days). Return an EMPTY frame that still carries the
        # {ep}__nma / {ep}__cmp columns, so callers get empty Series rather than a KeyError on a
        # column-less frame (the guarded callers check len()>0; the figure builders index by column).
        per_user = pd.DataFrame(columns=(["_userId"]
                                         + [f"{ep}__nma" for ep in ep_cols]
                                         + [f"{ep}__cmp" for ep in ep_cols]))
    coverage = {"total_nma_days": total_nma, "matched_nma_days": matched_nma,
                "n_users_matched": len(per_user)}
    return per_user, coverage
