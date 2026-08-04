"""Shared preset-characterization helpers for the IR analyses.

Extracted from analysis_ir-1_preset_characterization.py (2026-08-03) so
Analysis IR-3 (dataset-wide characterization on AB days, PLN IR-1002 §8.2) can
reuse the identical table machinery. IR-1 stratifies by transition period
(`segment`), IR-3 by guardrail status — everything else (parameter list,
summary-stat formatting, distinct-configuration identity, per-user usage
aggregation, small-cell flagging) is shared.

Two parameters generalize what IR-1 hard-coded:
- `_usage_rows(..., basis_label=...)` — IR-1's per-user rows are "per 14 days"
  (its fixed 14-day windows); IR-3 passes "per 14 eligible AB days" (a per-day
  rate scaled to a 14-day basis for comparability — see PLN §7.5).
- `per_user_usage(..., norm_days=...)` — IR-3 supplies each user's eligible
  AB-day count so activations and hours are normalized to the 14-day basis;
  IR-1 omits it (its windows are already exactly 14 days).
"""

import numpy as np
import pandas as pd


S_PER_HOUR = 3_600  # override durations are stored in seconds

# Basis used for per-user rate rows when the caller doesn't override it
# (IR-1's fixed 14-day analysis windows).
DEFAULT_BASIS_LABEL = "14 days"

# Days the per-user rates are expressed over when norm_days is supplied.
NORM_BASIS_DAYS = 14

# (display label, column, decimal places). gtm is derived at load time.
PARAMETERS = [
    ("Basal rate scale factor",           "basalRateScaleFactor",          3),
    ("Carb ratio scale factor",           "carbRatioScaleFactor",          3),
    ("Insulin sensitivity scale factor",  "insulinSensitivityScaleFactor", 3),
    ("Glucose target low (mg/dL)",        "bg_target_low",                 1),
    ("Glucose target high (mg/dL)",       "bg_target_high",                1),
    ("Glucose target midpoint (mg/dL)",   "gtm",                           1),
]

PARAM_COLS = [
    "basalRateScaleFactor", "carbRatioScaleFactor",
    "insulinSensitivityScaleFactor", "bg_target_low", "bg_target_high",
]

# Distinct-configuration identity: user + preset name + exact parameter set
# (mirrors the staging full-config key; NaNs compare equal in drop_duplicates).
CONFIG_KEY = ["_userId", "preset_name"] + PARAM_COLS

UNNAMED_PRESET = "(unnamed)"

SMALL_CELL_USERS = 5  # flag preset-name rows with fewer users than this

# Data-check comparison tolerances. The CR↔ISF tie is exact in the source (one
# UI dial writes both), so the tie check is tight; the basal-linkage checks
# tolerate rounding in how Loop derives the stored factors.
CRISF_TIE_RTOL = 1e-6
CRISF_TIE_ATOL = 1e-6
BASAL_LINKAGE_RTOL = 1e-3


def fmt(v, dp: int) -> str:
    return "—" if pd.isna(v) else f"{v:.{dp}f}"


def dist_row(values: pd.Series, n_users: int, dp: int) -> dict:
    """Mean ± SD, min–max, median [IQR] for one parameter/outcome cell."""
    values = values.dropna()
    n = len(values)
    if n == 0:
        return {
            "N": 0, "N users": n_users, "Mean ± SD": "—",
            "Min–Max": "—", "Median [IQR]": "—",
        }
    sd = values.std(ddof=1) if n > 1 else np.nan
    return {
        "N": n,
        "N users": n_users,
        "Mean ± SD": f"{fmt(values.mean(), dp)} ± {fmt(sd, dp)}",
        "Min–Max": f"{fmt(values.min(), dp)}–{fmt(values.max(), dp)}",
        "Median [IQR]": (
            f"{fmt(values.median(), dp)} "
            f"[{fmt(values.quantile(0.25), dp)}, {fmt(values.quantile(0.75), dp)}]"
        ),
    }


def per_user_usage(activations: pd.DataFrame, norm_days: pd.Series = None) -> pd.DataFrame:
    """One row per user with ≥1 activation in the window: activation count,
    total effective hours, and the count of duration-bearing activations.

    norm_days: optional per-user observation length (a Series indexed by
    _userId). When given, n_activations and total_hours are scaled to a
    NORM_BASIS_DAYS basis (rate x 14); n_with_duration stays a raw count so
    mean-duration-per-activation remains an unscaled per-activation quantity.
    Users with a non-positive or missing norm_days contribute NaN rates.
    """
    per_user = (
        activations.groupby("_userId")
        .agg(
            n_activations=("_userId", "size"),
            total_hours=("duration", lambda s: s.sum() / S_PER_HOUR),
            n_with_duration=("duration", "count"),
        )
    )
    # Unscaled hours survive for the mean-duration-per-activation row, which is
    # a per-activation quantity and must not inherit the rate scaling.
    per_user["total_hours_raw"] = per_user["total_hours"]
    if norm_days is not None:
        days = pd.to_numeric(norm_days.reindex(per_user.index), errors="coerce")
        days = days.where(days > 0)
        scale = NORM_BASIS_DAYS / days
        per_user["n_activations"] = per_user["n_activations"] * scale
        per_user["total_hours"] = per_user["total_hours"] * scale
    return per_user


def usage_rows(
    per_user: pd.DataFrame,
    stratum_label: str,
    stratum_col: str = "Period",
    basis_label: str = DEFAULT_BASIS_LABEL,
) -> list:
    """The three per-user outcome rows shared by the IR-1c/1d and IR-3c/3d
    tables.

    Mean duration per activation is restricted to users with ≥1
    duration-bearing activation in the stratum (0 ÷ 0 is undefined) and is
    computed from unscaled totals when rates are normalized.
    """
    n_users = len(per_user)
    with_dur = per_user["n_with_duration"] > 0
    hours_for_mean = per_user.get("total_hours_raw", per_user["total_hours"])
    mean_dur = (
        hours_for_mean.loc[with_dur] / per_user.loc[with_dur, "n_with_duration"]
    )
    return [
        {stratum_col: stratum_label,
         "Outcome": f"Preset activations per user (n/{basis_label})",
         **dist_row(per_user["n_activations"], n_users, 1)},
        {stratum_col: stratum_label,
         "Outcome": f"Total preset time per user (hours/{basis_label})",
         **dist_row(per_user["total_hours"], n_users, 1)},
        {stratum_col: stratum_label,
         "Outcome": "Mean duration per activation (hours)",
         **dist_row(mean_dur, len(mean_dur), 1)},
    ]


def prepare_activations(activations: pd.DataFrame) -> pd.DataFrame:
    """Numeric-coerce the parameter/duration columns and derive `gtm` (glucose
    target midpoint) and `preset_name` (free-text name, NULL -> UNNAMED_PRESET).
    Mutates and returns the frame."""
    numeric_cols = PARAM_COLS + ["duration"]
    if "stated_duration" in activations.columns:
        numeric_cols.append("stated_duration")
    else:
        activations["stated_duration"] = np.nan
    for col in numeric_cols:
        activations[col] = pd.to_numeric(activations[col], errors="coerce")
    activations["gtm"] = (
        activations["bg_target_low"] + activations["bg_target_high"]
    ) / 2.0
    activations["preset_name"] = activations["overridePreset"].fillna(UNNAMED_PRESET)
    return activations


def derive_insulin_needs(activations: pd.DataFrame) -> pd.DataFrame:
    """Add `insulin_needs_pct` — the overall insulin-needs multiplier, as a
    percentage of the user's scheduled therapy.

    Loop stores one "overall insulin needs" dial as three linked factors:
    basal = f, and CR = ISF = 1/f (verified 100% in IR-1f). The basal factor
    therefore IS the insulin-needs multiplier, while the CR/ISF factors are its
    RECIPROCAL — a CR factor of 10 means ten times the carb ratio, i.e. 10% of
    normal insulin, not 1000%. Reading the three factors as one scale would
    invert two of them, so everything collapses to this single quantity:
    basal factor when present, else 1/CR, else 1/ISF; x100 to read directly
    against the guardrail bounds (15-200%) and the mitigation threshold (170%).
    """
    brsf = activations["basalRateScaleFactor"]
    crsf = activations["carbRatioScaleFactor"]
    issf = activations["insulinSensitivityScaleFactor"]
    needs = brsf.where(brsf > 0)
    needs = needs.fillna(1.0 / crsf.where(crsf > 0))
    needs = needs.fillna(1.0 / issf.where(issf > 0))
    activations["insulin_needs_pct"] = needs * 100.0
    return activations


def parameter_distribution_rows(
    activations: pd.DataFrame, strata, stratum_col: str = "Period",
    parameters=None,
) -> list:
    """Parameter-distribution rows at both grains for each stratum.

    strata: iterable of (mask, label) — a boolean Series selecting the
    stratum's activations and its display label.
    parameters: (label, column, decimals) triples; defaults to PARAMETERS (the
    raw stored parameters, as IR-1 reports them). IR-3 passes its own list to
    report the collapsed insulin-needs quantity instead of the three factors.
    """
    if parameters is None:
        parameters = PARAMETERS
    rows = []
    for grain in ("activation", "distinct configuration"):
        for mask, label in strata:
            subset = activations[mask]
            if grain == "distinct configuration":
                subset = subset.drop_duplicates(subset=CONFIG_KEY)
            for param_label, col, dp in parameters:
                n_users = subset.loc[subset[col].notna(), "_userId"].nunique()
                rows.append({
                    "Grain": grain,
                    stratum_col: label,
                    "Parameter": param_label,
                    **dist_row(subset[col], n_users, dp),
                })
    return rows


def duration_rows(activations: pd.DataFrame, strata, stratum_col: str = "Period") -> list:
    """Activation-level effective + programmed duration rows per stratum, in
    hours."""
    outcomes = [
        ("Effective duration (hours)",  "duration"),
        ("Programmed duration (hours)", "stated_duration"),
    ]
    rows = []
    for mask, label in strata:
        subset = activations[mask]
        for outcome_label, col in outcomes:
            hours = subset[col] / S_PER_HOUR
            n_users = subset.loc[subset[col].notna(), "_userId"].nunique()
            rows.append({
                stratum_col: label,
                "Outcome": outcome_label,
                **dist_row(hours, n_users, 1),
            })
    return rows


def preset_name_rows(activations: pd.DataFrame) -> pd.DataFrame:
    """Per-preset-name usage breakdown with the small-cell flag, ordered by
    activation count (descending).

    ⚠ Preset names are user-entered free text — screen for identifying content
    (and small cells) before this table leaves the analysis environment.
    """
    rows = []
    for name, group in activations.groupby("preset_name"):
        n_users = group["_userId"].nunique()
        rows.append({
            "Preset name": name,
            "N activations": len(group),
            "N users": n_users,
            "Total effective hours": round(group["duration"].sum() / S_PER_HOUR, 1),
            "N distinct configurations": len(group.drop_duplicates(subset=CONFIG_KEY)),
            f"Small cell (<{SMALL_CELL_USERS} users)": n_users < SMALL_CELL_USERS,
        })
    table = pd.DataFrame(rows)
    if table.empty:
        return table
    table = (
        table.sort_values(["N activations", "Preset name"], ascending=[False, True])
        .reset_index(drop=True)
    )
    n_small = int(table[f"Small cell (<{SMALL_CELL_USERS} users)"].sum())
    if n_small:
        print(f"  ⚠ {n_small} preset-name rows have <{SMALL_CELL_USERS} users — "
              "screen before external use (free-text names)")
    return table


def linkage_checks(activations: pd.DataFrame) -> dict:
    """CR≡ISF tie and basal↔CR linkage counts (the shared half of the IR-1f /
    IR-3f data checks)."""
    crsf = activations["carbRatioScaleFactor"]
    issf = activations["insulinSensitivityScaleFactor"]
    brsf = activations["basalRateScaleFactor"]

    both_ci = crsf.notna() & issf.notna()
    ci_equal = np.isclose(
        crsf[both_ci], issf[both_ci], rtol=CRISF_TIE_RTOL, atol=CRISF_TIE_ATOL
    )

    # In Loop, one "overall insulin needs" multiplier f sets basal = f and
    # CR = ISF = 1/f; test both directions. Only the CR factor is compared here
    # — its tie to ISF is the separate check above.
    both_bc = brsf.notna() & crsf.notna() & (brsf > 0)
    bc_reciprocal = np.isclose(
        crsf[both_bc], 1.0 / brsf[both_bc], rtol=BASAL_LINKAGE_RTOL
    )
    bc_equal = np.isclose(crsf[both_bc], brsf[both_bc], rtol=BASAL_LINKAGE_RTOL)

    return {
        "n_both_ci": int(both_ci.sum()),
        "n_ci_equal": int(ci_equal.sum()),
        "n_both_bc": int(both_bc.sum()),
        "n_bc_reciprocal": int(bc_reciprocal.sum()),
        "n_bc_equal": int(bc_equal.sum()),
    }


def pct(k: int, n: int) -> str:
    """'k (x.y%)' against denominator n; '0' when n is 0."""
    return f"{k} ({100 * k / n:.1f}%)" if n else "0"
