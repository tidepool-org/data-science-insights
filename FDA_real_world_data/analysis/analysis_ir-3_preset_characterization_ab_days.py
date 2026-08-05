"""
=============================================================================
Analysis IR-3: Preset Characterization on Autobolus Days
FDA 510(k) Submission: Loop Autobolus Feature — PLN IR-1002 Analysis 2
=============================================================================

Objective: characterize the preset configurations in real-world use during
autobolus therapy, dataset-wide, relative to the Tidepool Loop 2.0
configuration bounds — the IR-1 characterization extended from the transition
cohort to the full eligible population, with the period dimension replaced by
GUARDRAIL STATUS (PLN IR-1002 §7.5/§8.2).

Inclusion: qualifying activations (version/date-eligible, on/after the user's
first eligible AB day) for which EVERY day the activation window touches is an
eligible AB day (`is_qualifying AND is_all_days_ab`). No CGM-coverage gate —
a preset's configuration exists regardless of sensor wear (PLN §7.1).

Stratification — activation-level guardrail status:
    All activations | Compliant | P-only | M-only | Both
(P = preset-guardrail violation: own target outside [67, 250] mg/dL or insulin
needs outside [15%, 200%]; M = mitigation violation: needs > 170% with the
effective target lower bound < 110 mg/dL at any point.)

Per-user rates (Tables IR-3c/3d) are per-eligible-AB-day rates scaled to a
14-day basis — presentational, for side-by-side reading with RPT-1001 Table
8.4a and IR-1 Tables IR-1c/1d, whose windows are literally 14 days (PLN §7.5).

Box-independent: reads the production IR-1002 staging tables only; no suffix.

Outputs (outputs/analysis_ir_3/):
- Table IR-3a: parameter distributions by guardrail status × grain
  (`table_ir3a_parameter_distributions.csv`)
- Table IR-3b: activation durations by guardrail status
  (`table_ir3b_activation_durations.csv`)
- Table IR-3c: per-user usage, zero-filled over the full cohort
  (`table_ir3c_per_user_full_cohort.csv`)
- Table IR-3d: per-user usage, preset users only
  (`table_ir3d_per_user_preset_users.csv`)
- Table IR-3f: data checks (`table_ir3f_data_checks.csv`)

Table IR-3e (per-preset-name breakdown) is DEFERRED (2026-08-04) — the
free-text names need screening before they are worth generating. The shared
`preset_name_rows()` helper builds it when wanted; the letter stays reserved so
the PLN §8.2 mapping does not shift.
=============================================================================
"""

import os

import pandas as pd

from utils.data_loading import CATALOG
from utils.preset_characterization import (
    derive_insulin_needs,
    dist_row,
    duration_rows,
    linkage_checks,
    parameter_distribution_rows,
    pct,
    per_user_usage,
    prepare_activations,
    usage_rows,
)

OUTPUT_DIR = "outputs/analysis_ir_3"

STRATUM_COL = "Guardrail status"

# (display label, column, decimal places). The three stored scale factors
# collapse to ONE insulin-needs quantity (see derive_insulin_needs: basal = f,
# CR = ISF = 1/f, so reporting them as three parallel rows inverts two of
# them). Expressed as a percentage so it reads against the guardrail bounds
# (15-200%) and the mitigation threshold (170%) directly.
IR3_PARAMETERS = [
    ("Overall insulin needs (%)",         "insulin_needs_pct", 1),
    ("Glucose target low (mg/dL)",        "bg_target_low",     1),
    ("Glucose target high (mg/dL)",       "bg_target_high",    1),
    ("Glucose target midpoint (mg/dL)",   "gtm",               1),
]

# Per-user rates are expressed per this label (PLN §7.5 presentational basis).
BASIS_LABEL = "14 eligible AB days"

# (status value, display label) — activation-level guardrail status, in report
# order. "all" is the union row, not a partition member.
STATUSES = [
    ("all",       "All activations"),
    ("compliant", "Compliant"),
    ("p_only",    "P-only"),
    ("m_only",    "M-only"),
    ("both",      "Both"),
]


# =============================================================================
# Data loading
# =============================================================================

def load_data(spark):
    """The IR-3 activation set (qualifying, all spanned days AB) with
    activation-level guardrail status, plus the cohort denominator and
    per-user eligible-AB-day counts (the rate normalizer)."""
    flags = spark.table(f"{CATALOG}.override_guardrail_flags").toPandas()
    flags = derive_insulin_needs(prepare_activations(flags))
    # Spark BOOLEANs can arrive as pandas nullable/object dtype; boolean
    # indexing on pd.NA raises, so normalize every flag up front.
    for col in ("is_qualifying", "is_all_days_ab", "is_p_violation",
                "is_m_violation", "is_m_indeterminate", "is_version_eligible",
                "is_after_first_ab"):
        if col in flags.columns:
            flags[col] = flags[col].fillna(False).astype(bool)

    qualifying = flags[flags["is_qualifying"]].copy()
    activations = qualifying[qualifying["is_all_days_ab"]].copy()

    is_p = activations["is_p_violation"]
    is_m = activations["is_m_violation"]
    activations["guardrail_status"] = "compliant"
    activations.loc[is_p & ~is_m, "guardrail_status"] = "p_only"
    activations.loc[~is_p & is_m, "guardrail_status"] = "m_only"
    activations.loc[is_p & is_m, "guardrail_status"] = "both"

    day_counts = spark.sql(f"""
        --begin-sql
        SELECT
          _userId,
          SUM(CASE WHEN is_eligible_ab_day THEN 1 ELSE 0 END) AS n_eligible_ab_days
        FROM {CATALOG}.ab_day_cohort
        GROUP BY _userId
        HAVING SUM(CASE WHEN is_eligible_ab_day THEN 1 ELSE 0 END) > 0
        ;
    """).toPandas()
    day_counts["n_eligible_ab_days"] = pd.to_numeric(
        day_counts["n_eligible_ab_days"], errors="coerce"
    ).astype(float)
    norm_days = day_counts.set_index("_userId")["n_eligible_ab_days"]

    print(f"  Qualifying activations: {len(qualifying)}")
    print(f"  … with every spanned day an eligible AB day: {len(activations)} "
          f"({activations['_userId'].nunique()} users)")
    print(f"  Cohort users (≥1 eligible AB day): {len(day_counts)}")
    print(f"  Status counts: {activations['guardrail_status'].value_counts().to_dict()}")
    return activations, qualifying, norm_days


def _status_strata(activations: pd.DataFrame):
    """(mask, label) per guardrail status, 'All activations' first."""
    strata = []
    for value, label in STATUSES:
        if value == "all":
            mask = pd.Series(True, index=activations.index)
        else:
            mask = activations["guardrail_status"] == value
        strata.append((mask, label))
    return strata


# =============================================================================
# Tables IR-3a / IR-3b — distributions by guardrail status
# =============================================================================

def create_table_ir3a(activations: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(parameter_distribution_rows(
        activations, _status_strata(activations), stratum_col=STRATUM_COL,
        parameters=IR3_PARAMETERS,
    ))


def create_table_ir3b(activations: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(duration_rows(
        activations, _status_strata(activations), stratum_col=STRATUM_COL
    ))


# =============================================================================
# Tables IR-3c / IR-3d — per-user usage
# =============================================================================

def create_table_ir3c(activations: pd.DataFrame, norm_days: pd.Series) -> pd.DataFrame:
    """Per-user frequency and preset time on eligible AB days, zero-filled over
    every cohort user (≥1 eligible AB day) — "the average user". Rates are per
    BASIS_LABEL (PLN §7.5)."""
    n_cohort = len(norm_days)
    per_user = (
        per_user_usage(activations, norm_days=norm_days)
        .reindex(norm_days.index)
        .fillna(0.0)
    )
    n_any = int((per_user["n_activations"] > 0).sum())

    rows = usage_rows(per_user, "Full cohort (zero-filled)",
                      stratum_col="Scope", basis_label=BASIS_LABEL)
    rows.append({
        "Scope": "Full cohort (zero-filled)",
        "Outcome": "Eligible AB days per user (n)",
        **dist_row(norm_days, n_cohort, 1),
    })
    rows.append({
        "Scope": "Full cohort (zero-filled)",
        # NOT "any preset use": the activation set additionally requires every
        # spanned day to be an eligible AB day (§7.5), so users whose every
        # qualifying activation crossed a non-AB day are zero-filled here and
        # counted as non-users of presets in this table.
        "Outcome": "Users contributing activations to this analysis, n (%)",
        "N": n_cohort, "N users": n_cohort,
        "Mean ± SD": f"{n_any} ({100 * n_any / n_cohort:.1f}%)" if n_cohort else "—",
        "Min–Max": "—", "Median [IQR]": "—",
    })
    return pd.DataFrame(rows).drop(columns=["Scope"])


def create_table_ir3d(activations: pd.DataFrame, norm_days: pd.Series) -> pd.DataFrame:
    """The same outcomes among preset users only (≥1 qualifying all-AB-day
    activation) — "the average preset user"."""
    per_user = per_user_usage(activations, norm_days=norm_days)
    rows = usage_rows(per_user, "Preset users only",
                      stratum_col="Scope", basis_label=BASIS_LABEL)
    rows.append({
        "Scope": "Preset users only",
        "Outcome": "Eligible AB days per user (n)",
        **dist_row(norm_days.reindex(per_user.index), len(per_user), 1),
    })
    return pd.DataFrame(rows).drop(columns=["Scope"])


# =============================================================================
# Table IR-3f — data checks
# =============================================================================

def create_table_ir3f(activations, qualifying, norm_days) -> pd.DataFrame:
    checks = linkage_checks(activations)
    n_set = len(activations)
    n_qualifying = len(qualifying)
    n_multiday_excluded = int((~qualifying["is_all_days_ab"]).sum())
    status_counts = activations["guardrail_status"].value_counts()

    rows = [
        ("Activations meeting the all-spanned-days-AB requirement / all "
         "qualifying activations", f"{n_set} / {n_qualifying}"),
        ("Activations excluded by the all-spanned-days-AB requirement, n",
            str(n_multiday_excluded)),
        ("Users contributing activations",
            pct(int(activations["_userId"].nunique()), len(norm_days))),
        ("Compliant activations", pct(int(status_counts.get("compliant", 0)), n_set)),
        ("P-only activations", pct(int(status_counts.get("p_only", 0)), n_set)),
        ("M-only activations", pct(int(status_counts.get("m_only", 0)), n_set)),
        ("Both (P and M) activations", pct(int(status_counts.get("both", 0)), n_set)),
        ("Indeterminate mitigation status: activations, n",
            str(int(activations["is_m_indeterminate"].sum()))),
        ("Activations that adjust insulin needs "
         "(carb-ratio and insulin-sensitivity both recorded)",
            str(checks["n_both_ci"])),
        ("… where the carb-ratio and insulin-sensitivity factors are equal",
            pct(checks["n_ci_equal"], checks["n_both_ci"])),
        ("Activations that adjust insulin needs "
         "(basal and carb-ratio both recorded)",
            str(checks["n_both_bc"])),
        ("… where the carb-ratio factor is the reciprocal of the basal factor",
            pct(checks["n_bc_reciprocal"], checks["n_both_bc"])),
        ("Indefinite overrides (no programmed duration), n",
            str(int(activations["stated_duration"].isna().sum()))),
        ("Activations with NULL effective duration (expect 0)",
            str(int(activations["duration"].isna().sum()))),
    ]
    return pd.DataFrame(rows, columns=["Check", "Value"])


# =============================================================================
# Main
# =============================================================================

def run_analysis(spark, output_dir=None):
    if output_dir is None:
        output_dir = OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis IR-3: Preset Characterization on Autobolus Days")
    print("=" * 60)

    print("\n1. Loading data...")
    activations, qualifying, norm_days = load_data(spark)

    print("\n2. Table IR-3a — parameter distributions by guardrail status...")
    table_ir3a = create_table_ir3a(activations)
    table_ir3a.to_csv(f"{output_dir}/table_ir3a_parameter_distributions.csv", index=False)
    print(table_ir3a.to_string(index=False))

    print("\n3. Table IR-3b — activation durations by guardrail status...")
    table_ir3b = create_table_ir3b(activations)
    table_ir3b.to_csv(f"{output_dir}/table_ir3b_activation_durations.csv", index=False)
    print(table_ir3b.to_string(index=False))

    print("\n4. Table IR-3c — per-user usage, full cohort (zero-filled)...")
    table_ir3c = create_table_ir3c(activations, norm_days)
    table_ir3c.to_csv(f"{output_dir}/table_ir3c_per_user_full_cohort.csv", index=False)
    print(table_ir3c.to_string(index=False))

    print("\n5. Table IR-3d — per-user usage, preset users only...")
    table_ir3d = create_table_ir3d(activations, norm_days)
    table_ir3d.to_csv(f"{output_dir}/table_ir3d_per_user_preset_users.csv", index=False)
    print(table_ir3d.to_string(index=False))

    print("\n6. Table IR-3f — data checks...")
    table_ir3f = create_table_ir3f(activations, qualifying, norm_days)
    table_ir3f.to_csv(f"{output_dir}/table_ir3f_data_checks.csv", index=False)
    print(table_ir3f.to_string(index=False))

    print("\n" + "=" * 60)
    print("Analysis IR-3 Complete!")
    print("=" * 60)

    return {
        "table_ir3a": table_ir3a,
        "table_ir3b": table_ir3b,
        "table_ir3c": table_ir3c,
        "table_ir3d": table_ir3d,
        "table_ir3f": table_ir3f,
        "activations": activations,
        "norm_days": norm_days,
    }


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    import argparse

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--output-dir", default=OUTPUT_DIR)
    _args, _ = _parser.parse_known_args()
    run_analysis(spark, output_dir=_args.output_dir)  # type: ignore[name-defined]
