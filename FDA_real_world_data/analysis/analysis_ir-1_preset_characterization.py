"""
=============================================================================
Analysis IR-1: Characterization of Configurable Presets
FDA 510(k) Submission: Loop Autobolus Feature — interactive review response
=============================================================================

Objective: Characterize the preset overrides observed during the TB→AB
transition windows that underlie Analyses 8.1 / 8.2 / 8.4, answering the
FDA interactive-review question (2026-07-30): (a) the preset settings
available in the dataset, (b) the distribution of each preset parameter
during the temp-basal and autobolus periods, and (c) the frequency and
duration of preset activations per user during each period.

Descriptive only — no inferential gates. Unlike Analysis 8.3, activations
are NOT filtered on pairing validity (is_valid_name_only_*) or starting
glucose; every activation by an eligible cohort user inside a transition
segment window is characterized. The TB-vs-AB *change* analysis remains
Analysis 8.3; the per-user paired usage comparison remains Analysis 8.4.

Inclusion:
- Eligible transition segments from load_allowed_transition_segments
  (analysis cohort gate = Loop version, guardrail exclusion, type-1
  diagnosis) — the same gates Analyses 8.3 / 8.4 apply.
- All three segment windows are reported separately: temp basal (seg1),
  initial autobolus (seg2, days 0–14), second autobolus (seg3, days 14–28).
  `segment` is used for the split — never `dosing_mode`, which collapses
  seg2 + seg3 into a single 'autobolus' label.

Grains:
- "activation" (primary): each activation contributes once.
- "distinct configuration": one row per (user, preset name, exact parameter
  set) per period — the settings in use, not weighted by how often used.

Durations: `duration` is the EFFECTIVE duration — bounded upstream by
min(stated, gap to next override, time to segment end) — i.e. in-window
exposure. `stated_duration` is the as-programmed value. Both are reported;
staging tables built before stated_duration existed yield a "—" row.
A NULL programmed duration (indefinite override / unparseable) does NOT
propagate: Spark LEAST skips NULLs, so the effective duration falls back
to the gap-to-next or segment-end bound. Table IR-1f counts those rows;
the effective-duration rows in IR-1b include them, the programmed rows
cannot.

Denominator note: the per-user cohort denominator spans every eligible
segment (any rank) while overrides_by_segment holds rank-1 windows only —
identical to Table 8.4a's design. A user whose rank-1 window is
gate-excluded appears in the denominator with zero activations.

Outputs (outputs/analysis_ir_1{suffix}/) — one table per letter. NOTE: the
letters were reshuffled once (2026-07-30, when IR-1d split out); delete any
older-vintage table_ir1*.csv from a previously-used output dir by hand so a
letter doesn't appear twice:
- Table IR-1a: preset parameter distributions by period × grain
  (`table_ir1a_parameter_distributions.csv`)
- Table IR-1b: activation-level durations by period
  (`table_ir1b_activation_durations.csv`)
- Table IR-1c: per-user activation frequency / preset time by period,
  zero-filled over the FULL eligible cohort — the average user, with
  non-users contributing 0 (Table 8.4a's denominator design)
  (`table_ir1c_per_user_full_cohort.csv`)
- Table IR-1d: the same per-user outcomes among PRESET USERS only — users
  with ≥1 activation in that window, no zero-fill — the average preset
  user (`table_ir1d_per_user_preset_users.csv`)
- Table IR-1e: per-preset-name usage breakdown
  (`table_ir1e_preset_name_breakdown.csv`) — ⚠ preset names are
  user-entered free text; screen for identifying content and small cells
  before any external use.
- Table IR-1f: data checks backing the response prose (CR≡ISF tie,
  basal↔CR/ISF linkage, NULL durations, preset users inside the 8.1
  cohort) (`table_ir1f_data_checks.csv`)
=============================================================================
"""

import os

import pandas as pd

from utils.data_loading import (
    CATALOG,
    load_allowed_transition_segments,
    load_transition_endpoints,
)
from utils.preset_characterization import (
    CONFIG_KEY as _CONFIG_KEY,
    S_PER_HOUR,
    SMALL_CELL_USERS,
    duration_rows,
    linkage_checks,
    parameter_distribution_rows,
    pct as _pct,
    per_user_usage as _per_user_usage,
    prepare_activations,
    usage_rows as _usage_rows,
)

OUTPUT_DIR = "outputs/analysis_ir_1"

# The unsuffixed production build IS the report primary (0.80 box) as of
# 2026-08-05, so a bare Run-file analyzes it — same default as every other
# analysis. Pass --suffix _box070 / _box090 for the sensitivity builds.
DEFAULT_SUFFIX = ""

# (segment value, display label) — report each period separately.
PERIODS = [
    ("tb_to_ab_seg1", "Temp basal period"),
    ("tb_to_ab_seg2", "Initial autobolus period (days 0–14)"),
    ("tb_to_ab_seg3", "Second autobolus period (days 14–28)"),
]


# =============================================================================
# Data loading
# =============================================================================

def load_data(spark, suffix: str = ""):
    """
    Load every preset activation by an eligible transition-cohort user,
    plus the full cohort user list (frequency denominator) and the final
    Analysis 8.1 cohort user set (for the preset-exposure-in-8.1 check).

    suffix='_box080' reads the parallel 0.80-box cohort tables.
    """
    # Cohort gate + guardrail exclusion + type-1 diagnosis — the same eligible
    # segments Analyses 8.3 / 8.4 build from.
    allowed_segments = load_allowed_transition_segments(spark, suffix=suffix)

    cohort_users = allowed_segments.select("_userId").distinct().toPandas()

    # All override events for eligible segments — no validity or
    # starting-glucose filter (descriptive characterization).
    overrides = (
        spark.table(f"{CATALOG}.overrides_by_segment{suffix}")
        .join(allowed_segments, on=["_userId", "tb_to_ab_seg1_start"], how="inner")
        .toPandas()
    )

    if "stated_duration" not in overrides.columns:
        print("  Warning: stated_duration not in overrides_by_segment — "
              "re-run export_overrides_from_transitions to populate it; "
              "programmed-duration rows will be empty")
    overrides = prepare_activations(overrides)

    # Final 8.1 cohort (adds CGM coverage + paired halves on top of the gates
    # above) — used only for the Table IR-1f preset-exposure check.
    endpoints_81 = load_transition_endpoints(spark, suffix=suffix)
    users_81 = set(endpoints_81["_userId"])

    print(f"  Eligible transition users: {len(cohort_users)}")
    print(f"  Preset activations loaded: {len(overrides)} "
          f"({overrides['_userId'].nunique()} users)")
    return overrides, cohort_users, users_81


# =============================================================================
# Table IR-1a — parameter distributions by period × grain
# =============================================================================

def _period_strata(overrides: pd.DataFrame):
    """(mask, label) per period — the stratification the shared table builders
    take."""
    return [(overrides["segment"] == seg, label) for seg, label in PERIODS]


def create_table_ir1a(overrides: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        parameter_distribution_rows(overrides, _period_strata(overrides))
    )


# =============================================================================
# Table IR-1b — activation-level durations
# =============================================================================

def create_table_ir1b(overrides: pd.DataFrame) -> pd.DataFrame:
    """Activation-level duration distributions by period, in hours."""
    return pd.DataFrame(duration_rows(overrides, _period_strata(overrides)))


# =============================================================================
# Tables IR-1c / IR-1d — per-user frequency and preset time
# =============================================================================

def create_table_ir1c(
    overrides: pd.DataFrame, cohort_users: pd.DataFrame
) -> pd.DataFrame:
    """
    Per-user activation frequency and preset time by period, zero-filled
    over the FULL eligible cohort — "the average user": users with no
    activations in a period contribute 0 (not excluded), the same
    denominator design as Table 8.4a, here per segment window rather than
    per dosing mode. Table IR-1d repeats the outcomes among preset users
    only.
    """
    n_cohort = cohort_users["_userId"].nunique()
    rows = []
    for seg, period_label in PERIODS:
        in_period = overrides[overrides["segment"] == seg]
        # Zero-fill over the full cohort.
        per_user = _per_user_usage(in_period).reindex(cohort_users["_userId"]).fillna(
            {"n_activations": 0, "total_hours": 0, "n_with_duration": 0}
        )
        n_any = int((per_user["n_activations"] > 0).sum())

        rows.extend(_usage_rows(per_user, period_label))
        # N / N users mirror the zero-filled rows above (the whole cohort
        # contributes a yes/no); the count itself lives in the value column.
        rows.append({
            "Period": period_label,
            "Outcome": "Users with any preset use, n (%)",
            "N": n_cohort, "N users": n_cohort,
            "Mean ± SD": f"{n_any} ({100 * n_any / n_cohort:.1f}%)" if n_cohort else "—",
            "Min–Max": "—", "Median [IQR]": "—",
        })
    return pd.DataFrame(rows)


def create_table_ir1d(overrides: pd.DataFrame) -> pd.DataFrame:
    """
    The same per-user outcomes restricted to users with ≥1 activation in
    the window — "the average preset user". No zero-fill: the frequency and
    total-time rows describe preset users, not the whole cohort, so each
    period's N is that window's preset-user count.
    """
    rows = []
    for seg, period_label in PERIODS:
        in_period = overrides[overrides["segment"] == seg]
        rows.extend(_usage_rows(_per_user_usage(in_period), period_label))
    return pd.DataFrame(rows)


# =============================================================================
# Table IR-1e — per-preset-name breakdown
# =============================================================================

def create_table_ir1e(overrides: pd.DataFrame) -> pd.DataFrame:
    """
    Usage by preset name and period. ⚠ Preset names are user-entered free
    text — screen for identifying content (and small cells) before this
    table leaves the analysis environment.
    """
    rows = []
    for (name, seg), group in overrides.groupby(["preset_name", "segment"]):
        period_label = dict(PERIODS).get(seg)
        if period_label is None:
            continue
        n_users = group["_userId"].nunique()
        rows.append({
            "Preset name": name,
            "Period": period_label,
            "N activations": len(group),
            "N users": n_users,
            "Total effective hours": round(group["duration"].sum() / S_PER_HOUR, 1),
            "N distinct configurations": len(group.drop_duplicates(subset=_CONFIG_KEY)),
            f"Small cell (<{SMALL_CELL_USERS} users)": n_users < SMALL_CELL_USERS,
        })
    table = pd.DataFrame(rows)
    if table.empty:
        return table
    totals = (
        table.groupby("Preset name")["N activations"].sum()
        .sort_values(ascending=False)
    )
    # Periods sort chronologically (TB → seg2 → seg3), not alphabetically.
    period_rank = {label: i for i, (_, label) in enumerate(PERIODS)}
    table["_order"] = table["Preset name"].map(totals)
    table["_period_rank"] = table["Period"].map(period_rank)
    table = (
        table.sort_values(
            ["_order", "Preset name", "_period_rank"], ascending=[False, True, True]
        )
        .drop(columns=["_order", "_period_rank"])
        .reset_index(drop=True)
    )
    n_small = int(table[f"Small cell (<{SMALL_CELL_USERS} users)"].sum())
    if n_small:
        print(f"  ⚠ {n_small} preset-name rows have <{SMALL_CELL_USERS} users — "
              "screen before external use (free-text names)")
    return table


# =============================================================================
# Table IR-1f — data checks
# =============================================================================

def create_table_ir1f(
    overrides: pd.DataFrame, cohort_users: pd.DataFrame, users_81: set
) -> pd.DataFrame:
    """Empirical checks backing the response prose."""
    checks = linkage_checks(overrides)

    preset_users = set(overrides["_userId"])
    n_cohort = cohort_users["_userId"].nunique()

    rows = [
        ("Activations characterized (all periods)", str(len(overrides))),
        ("Users with ≥1 activation", _pct(len(preset_users), n_cohort)),
        ("Eligible transition users (denominator)", str(n_cohort)),
        ("Analysis 8.1 final cohort N", str(len(users_81))),
        ("8.1-cohort users with ≥1 activation in their rank-1 transition window",
            _pct(len(preset_users & users_81), len(users_81))),
        ("Activations with CR and ISF factors both present", str(checks["n_both_ci"])),
        ("… where CR factor = ISF factor",
            _pct(checks["n_ci_equal"], checks["n_both_ci"])),
        ("Activations with basal and carb-ratio factors both present (basal > 0)",
            str(checks["n_both_bc"])),
        ("… where carb-ratio factor = 1 / basal factor",
            _pct(checks["n_bc_reciprocal"], checks["n_both_bc"])),
        ("… where carb-ratio factor = basal factor",
            _pct(checks["n_bc_equal"], checks["n_both_bc"])),
        ("Activations with NULL programmed duration "
         "(effective falls back to gap / segment-end bound)",
            str(int(overrides["stated_duration"].isna().sum()))),
        ("Activations with NULL effective duration "
         "(expect 0 — staging bounds every duration)",
            str(int(overrides["duration"].isna().sum()))),
    ]
    return pd.DataFrame(rows, columns=["Check", "Value"])


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir=None, suffix: str = ""):
    # suffix='_box080' runs on the parallel 0.80-box cohort and writes to a
    # parallel output dir so the production outputs aren't clobbered.
    if output_dir is None:
        output_dir = OUTPUT_DIR + suffix
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis IR-1: Characterization of Configurable Presets")
    print("=" * 60)

    print("\n1. Loading data...")
    overrides, cohort_users, users_81 = load_data(spark, suffix=suffix)

    print("\n2. Table IR-1a — parameter distributions...")
    table_ir1a = create_table_ir1a(overrides)
    table_ir1a.to_csv(f"{output_dir}/table_ir1a_parameter_distributions.csv", index=False)
    print(table_ir1a.to_string(index=False))

    print("\n3. Table IR-1b — activation-level durations...")
    table_ir1b = create_table_ir1b(overrides)
    table_ir1b.to_csv(f"{output_dir}/table_ir1b_activation_durations.csv", index=False)
    print(table_ir1b.to_string(index=False))

    print("\n4. Table IR-1c — per-user usage, full cohort (zero-filled)...")
    table_ir1c = create_table_ir1c(overrides, cohort_users)
    table_ir1c.to_csv(f"{output_dir}/table_ir1c_per_user_full_cohort.csv", index=False)
    print(table_ir1c.to_string(index=False))

    print("\n5. Table IR-1d — per-user usage, preset users only...")
    table_ir1d = create_table_ir1d(overrides)
    table_ir1d.to_csv(f"{output_dir}/table_ir1d_per_user_preset_users.csv", index=False)
    print(table_ir1d.to_string(index=False))

    print("\n6. Table IR-1e — per-preset-name breakdown...")
    table_ir1e = create_table_ir1e(overrides)
    table_ir1e.to_csv(f"{output_dir}/table_ir1e_preset_name_breakdown.csv", index=False)

    print("\n7. Table IR-1f — data checks...")
    table_ir1f = create_table_ir1f(overrides, cohort_users, users_81)
    table_ir1f.to_csv(f"{output_dir}/table_ir1f_data_checks.csv", index=False)
    print(table_ir1f.to_string(index=False))

    print("\n" + "=" * 60)
    print("Analysis IR-1 Complete!")
    print("=" * 60)

    return {
        "table_ir1a": table_ir1a,
        "table_ir1b": table_ir1b,
        "table_ir1c": table_ir1c,
        "table_ir1d": table_ir1d,
        "table_ir1e": table_ir1e,
        "table_ir1f": table_ir1f,
        "overrides": overrides,
        "cohort_users": cohort_users,
    }


def run_in_databricks(spark, suffix: str = DEFAULT_SUFFIX):
    return run_analysis(spark, suffix=suffix)


if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--suffix", default=DEFAULT_SUFFIX,
                         help="source-table suffix (default '' = the report-primary "
                              "0.80 build); use _box070 / _box090 for sensitivity builds")
    _args, _ = _parser.parse_known_args()
    run_in_databricks(spark, suffix=_args.suffix)  # type: ignore[name-defined]
