"""Analysis for PLN-RFAI-0001: glycemic outcomes by dosing strategy and insulin
class (rapid-acting vs ultra-rapid-acting) in real-world Tidepool Loop use.

SCAFFOLD (2026-09-21; terminology aligned with PLN draft 6 on 2026-09-22: "subgroup" = one combination of dosing
strategy, insulin group, and age band; "per-user observation" = one user's pooled value within a subgroup). The pure-pandas machinery below is implemented and unit-
tested (testing/analysis/test_analysis_rfai_insulin_class_outcomes_units.py);
the Spark loaders are written against the Phase 1 table contracts of PLN
Appendix C and run once those tables are staged; the figure builders and the
final table shells are stubs that raise NotImplementedError until the next pass
aligns them with PLN §8 (Appendix E shells). run_analysis() halts on any hard
check (PLN §9.B) on the synthetic fixture and on production alike.

Two analyses (PLN §7):
  Analysis 1 — every outcome-eligible single-brand dosing day (both strategies),
      pooled per user x strategy x insulin group at the reading level with the
      construction of the interactive-review Analysis IR-2; confidence
      intervals, no tests. Cells coarser than the staged fine grain are
      re-pooled here by the count identities (repool) and verified against the
      staged class grain.
  Analysis 2 — the RPT-1001 §8.1 paired transition analysis re-run within each
      insulin group (window label from insulin_type_by_day, Supplement S1
      rule), the §8.1 TIR non-inferiority test per group, and a Welch interval
      on the URAI - RAI difference of paired TIR differences (primary
      contrast), read against a reference band (no equivalence test).

Inputs (Phase 1 staging; PLN Appendix C):
  {CATALOG}.insulin_type_by_day                            (user, day) -> insulin_type, insulin_class, n_brands, ...
  {CATALOG}.dosing_day_insulin_cohort                      one row per eligible dosing day (Table 8.1a source)
  {CATALOG}.glycemic_endpoints_dosing_days_insulin_fine    group: _userId, dosing_mode, insulin_class, insulin_type, age_band
  {CATALOG}.glycemic_endpoints_dosing_days_insulin_class   group: _userId, dosing_mode, insulin_class
  {CATALOG}.glycemic_endpoints_ab_days                     existing; hard check 4
  outputs/analysis_8_1/table_8_1a_*.csv                    production RPT-1001 §8.1 tables; hard check 1
Outputs: outputs/analysis_rfai_insulin_class_outcomes/ — table_8_1a_population.csv ...
  CSVs are long format (one row per strategy x insulin group); the report's .qmd pivots them to the house framing —
  dosing strategy outermost, insulin groups nested under each, as a two-level header (PLN draft 7, 2026-09-22).
  figure_8_2c_paired_differences.png (PLN numbering). No raw _userId in any file.

Hard checks (PLN §9.B), where they run:
  1 all-users Analysis 2 block reproduces the production Tables 8.1a/8.1b     -> here (check_reproduces_production_8_1)
  2 Analysis 2 group counts equal Supplement S1 Table 1                        -> RENDER TIME (the report's .qmd reads the
                                                                                 S1 export; dataset counts are never pinned in code)
  3 Analysis 1 day / user totals equal S1 Tables 2, 3 and Table IR-2a row 4    -> here for IR-2a (production CSV); S1 at render time
  4 users with no excluded autobolus day re-pool to glycemic_endpoints_ab_days -> here
  5 staged class grain equals the re-pooled fine grain on every per-user observation     -> here (check_repool_matches_class_grain)
  6 partition invariants on days and readings                                  -> here (load_layer1)
  7 no raw _userId in any written CSV                                          -> here (write_csv)
"""
import importlib.util
import os
import shutil
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import stats

from utils.data_loading import load_transition_endpoints
from utils.statistics import (
    compute_paired_statistics, hodges_lehmann_ci, paired_non_inferiority, welch_difference_ci,
)

# ---------------------------------------------------------------------------
# Constants (PLN-RFAI-0001 §5–§7; hoisted, never inlined)
# ---------------------------------------------------------------------------
CATALOG = "dev.fda_510k_rwd"
OUTPUT_DIR = "outputs/analysis_rfai_insulin_class_outcomes"
PRODUCTION_8_1_DIR = "outputs/analysis_8_1"          # RPT-1001 §8.1 tables of record (hard check 1)
PRODUCTION_IR_2_DIR = "outputs/analysis_ir_2"        # Table IR-2a (hard check 3)

INSULIN_TYPE_BY_DAY = f"{CATALOG}.insulin_type_by_day"
DOSING_DAY_COHORT = f"{CATALOG}.dosing_day_insulin_cohort"
ENDPOINTS_FINE = f"{CATALOG}.glycemic_endpoints_dosing_days_insulin_fine"
ENDPOINTS_CLASS = f"{CATALOG}.glycemic_endpoints_dosing_days_insulin_class"
ENDPOINTS_AB_DAYS = f"{CATALOG}.glycemic_endpoints_ab_days"

STRATEGIES = ["temp_basal", "autobolus"]
CLASS_MEMBERS = {"RAI": ["Novolog", "Humalog"], "URAI": ["Fiasp", "Lyumjev"]}   # labelled classes
APPENDIX_BRANDS = ["Apidra"]                                                    # rapid-acting, not on the proposed label
# Analysis 2 groups: (display label, level, label in the data)
GROUPS = [("RAI", "class", "RAI"), ("URAI", "class", "URAI"), ("Fiasp", "brand", "Fiasp"), ("Lyumjev", "brand", "Lyumjev")]
PRIMARY_CONTRAST = ("URAI", "RAI")                    # difference of paired TIR differences
AGE_BANDS = ["6-11", "12-17", "18+", "age unknown"]
PEDIATRIC_BANDS = ["6-11", "12-17"]

# Range metrics as staged by compute_glycemic_endpoints (percent of readings) plus derived rates.
BAND_COLS = ["tbr_very_low", "tbr", "tir", "tar", "tar_very_high"]
ENDPOINTS: List[Tuple[str, str]] = [
    ("tbr_very_low", "Time < 54 mg/dL (%)"), ("tbr", "Time < 70 mg/dL (%)"), ("tir", "Time 70–180 mg/dL (%)"),
    ("tar", "Time > 180 mg/dL (%)"), ("tar_very_high", "Time > 250 mg/dL (%)"), ("mean_glucose", "Mean glucose (mg/dL)"),
    ("cv", "Coefficient of variation (%)"), ("hypo_rate_per_hour", "Hypoglycemic events per CGM-hour"),
    ("hypo_per_day", "Hypoglycemic events per included day"),
]
MIN_PER_READING = 5                                   # CGM cadence: cbg_count -> hours
SMALL_SUBGROUP_USERS = 5                               # PLN §6.3 small-subgroup rule (IR-6B convention): distributional statistics withheld below this
MIN_DAYS_SENSITIVITY = 14                             # Table 8.1d: >= 14 outcome days per per-user observation
NI_MARGIN_TIR = 5.0                                   # percentage points, RPT-1001 §8.1
REPOOL_TOL = 1e-6
REPRODUCTION_TOL = 1e-6


# ---------------------------------------------------------------------------
# Pure-pandas machinery (unit-tested)
# ---------------------------------------------------------------------------
def repool(fine: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    """Pool per-subgroup endpoints to a coarser grain by the count identities.

    Each fine row is one (user, subgroup) with `cbg_count` readings, band shares
    in percent, `mean_glucose`, sample-SD `cv` (percent of the mean),
    `hypo_events`, and `n_days`. The pooled row equals what
    compute_glycemic_endpoints would return on the union of the readings:
      band share = reading-weighted mean of shares; mean = reading-weighted mean;
      hypo_events and n_days sum; CV from the pooled sample variance
      (sum of squares of every fine subgroup, then N-1 normalisation).
    """
    f = fine.copy()
    f["_sd"] = f["cv"] * f["mean_glucose"] / 100.0
    f["_ss"] = (f["cbg_count"] - 1) * f["_sd"] ** 2 + f["cbg_count"] * f["mean_glucose"] ** 2
    for col in BAND_COLS + ["mean_glucose"]:
        f[f"_w_{col}"] = f[col] * f["cbg_count"]
    agg = f.groupby(group_cols, dropna=False).agg(
        cbg_count=("cbg_count", "sum"), hypo_events=("hypo_events", "sum"), n_days=("n_days", "sum"), _ss=("_ss", "sum"),
        **{f"_w_{col}": (f"_w_{col}", "sum") for col in BAND_COLS + ["mean_glucose"]},
    ).reset_index()
    for col in BAND_COLS + ["mean_glucose"]:
        agg[col] = agg[f"_w_{col}"] / agg["cbg_count"]
    variance = (agg["_ss"] - agg["cbg_count"] * agg["mean_glucose"] ** 2) / (agg["cbg_count"] - 1)
    agg["cv"] = np.sqrt(variance.clip(lower=0)) * 100.0 / agg["mean_glucose"]
    return agg[group_cols + ["cbg_count", "n_days", "hypo_events"] + BAND_COLS + ["mean_glucose", "cv"]]


def add_hypo_rates(subgroups: pd.DataFrame) -> pd.DataFrame:
    """Hypoglycemic-event rates per CGM-hour and per included day (PLN §5)."""
    out = subgroups.copy()
    out["cgm_hours"] = out["cbg_count"] * MIN_PER_READING / 60.0
    out["hypo_rate_per_hour"] = out["hypo_events"] / out["cgm_hours"]
    out["hypo_per_day"] = out["hypo_events"] / out["n_days"]
    return out


def summarize_subgroups(per_user_observations: pd.DataFrame, subgroup_cols: List[str],
                    endpoint_cols: List[str] = None) -> pd.DataFrame:
    """Per-user pooled endpoints summarised within each subgroup: mean, SD, median,
    quartiles, users, days, CGM hours. Cells with fewer than SMALL_SUBGROUP_USERS
    users keep their counts and have every distributional statistic withheld
    (NaN) with `withheld = True` (PLN §6.3)."""
    endpoint_cols = endpoint_cols or [c for c, _ in ENDPOINTS]
    rows = []
    for key, grp in per_user_observations.groupby(subgroup_cols, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        base = dict(zip(subgroup_cols, key))
        base.update(n_users=len(grp), n_days=int(grp["n_days"].sum()), cgm_hours=float(grp["cgm_hours"].sum()))
        withheld = len(grp) < SMALL_SUBGROUP_USERS
        base["withheld"] = withheld
        for col in endpoint_cols:
            v = grp[col].dropna()
            n = len(v)
            half = stats.t.ppf(0.975, n - 1) * v.std(ddof=1) / np.sqrt(n) if n >= 2 else np.nan
            stats_ = {"mean": v.mean(), "sd": v.std(ddof=1), "ci_low": v.mean() - half, "ci_high": v.mean() + half,
                      "median": v.median(), "q1": v.quantile(0.25), "q3": v.quantile(0.75)}
            for k, val in stats_.items():
                base[f"{col}_{k}"] = np.nan if withheld else val
        rows.append(base)
    return pd.DataFrame(rows)


PARAMETRIC_SUFFIXES = ("_mean", "_sd", "_ci_low", "_ci_high")
NONPARAMETRIC_SUFFIXES = ("_median", "_q1", "_q3")


def split_parametric_nonparametric(summary: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """The two parts of every Analysis 1 outcome table (PLN §8, draft 5; the
    RPT-1001 Table 8.1a / 8.1b convention): parametric = mean ± SD with the 95%
    t-interval of the mean; nonparametric = median [IQR]. Counts and the
    withheld flag appear in both."""
    common = [c for c in summary.columns if not c.endswith(PARAMETRIC_SUFFIXES + NONPARAMETRIC_SUFFIXES)]
    parametric = summary[common + [c for c in summary.columns if c.endswith(PARAMETRIC_SUFFIXES)]]
    nonparametric = summary[common + [c for c in summary.columns if c.endswith(NONPARAMETRIC_SUFFIXES)]]
    return parametric, nonparametric


def window_insulin_group(label: str) -> str:
    """Analysis 2 group of a window label (Supplement S1 rule): a labelled
    class, or the label itself for Apidra / multiple / unknown (excluded from
    the groups, reported in the flow)."""
    for cls, members in CLASS_MEMBERS.items():
        if label in members:
            return cls
    return label


# ---------------------------------------------------------------------------
# Hard checks
# ---------------------------------------------------------------------------
def check_repool_matches_class_grain(fine: pd.DataFrame, class_grain: pd.DataFrame) -> None:
    """Hard check 5: the staged class grain equals the re-pooled fine grain on
    every (user, strategy, class) subgroup, to REPOOL_TOL."""
    key = ["_userId", "dosing_mode", "insulin_class"]
    pooled = repool(fine, key).set_index(key).sort_index()
    staged = class_grain.set_index(key).sort_index()
    assert len(pooled) == len(staged) and pooled.index.equals(staged.index), (
        f"class grain has {len(staged)} per-user observations, re-pooled fine grain has {len(pooled)}")
    for col in ["cbg_count", "hypo_events"] + BAND_COLS + ["mean_glucose", "cv"]:
        gap = (pooled[col].astype(float) - staged[col].astype(float)).abs().max()
        assert gap <= REPOOL_TOL, f"re-pooling does not reproduce the class grain on `{col}` (max gap {gap})"


def check_no_raw_user_ids(frame: pd.DataFrame, name: str) -> None:
    """Hard check 7: a written frame carries no raw user identifier."""
    bad = [c for c in frame.columns if c.lower().replace("_", "") in ("userid", "user")]
    assert not bad, f"{name}: raw user-id column(s) {bad} must not be written"


def write_csv(frame: pd.DataFrame, output_dir: str, name: str) -> None:
    check_no_raw_user_ids(frame, name)
    frame.to_csv(os.path.join(output_dir, name), index=False)
    print(f"  Saved {name} ({len(frame)} rows)")


def _load_module_from_path(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_reproduces_production_8_1(wide_all_users: pd.DataFrame, production_dir: str = PRODUCTION_8_1_DIR) -> None:
    """Hard check 1: the all-users Analysis 2 block, built with the RPT-1001
    §8.1 script's own table function on the same loader output, equals the
    production Table 8.1a CSVs subgroup for subgroup (numeric fields to
    REPRODUCTION_TOL). With the transition cohort one user smaller than the
    submitted table (Supplement S1 §4.3), this compares against the §8.1
    outputs regenerated on the current data, not the submitted PDF."""
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [f for f in os.listdir(here) if f.startswith("analysis_8-1") and f.endswith(".py")]
    assert len(candidates) == 1, candidates
    module_8_1 = _load_module_from_path(os.path.join(here, candidates[0]), "analysis_8_1")
    parametric, nonparametric = module_8_1.create_table_8_1a(wide_all_users)
    for built, filename in [(parametric, "table_8_1a_parametric.csv"), (nonparametric, "table_8_1a_nonparametric.csv")]:
        production = pd.read_csv(os.path.join(production_dir, filename))
        assert list(built.columns) == list(production.columns) and len(built) == len(production), filename
        for col in built.columns:
            b, p = built[col], production[col]
            if pd.api.types.is_numeric_dtype(b) and pd.api.types.is_numeric_dtype(p):
                assert np.allclose(b.astype(float), p.astype(float), atol=REPRODUCTION_TOL, equal_nan=True), (filename, col)
            else:
                assert (b.astype(str).values == p.astype(str).values).all(), (filename, col)
    print("  Hard check 1 passed: all-users block reproduces the production Table 8.1a files")


# ---------------------------------------------------------------------------
# Loaders (run once the Phase 1 tables exist)
# ---------------------------------------------------------------------------
def load_layer1(spark) -> Dict[str, pd.DataFrame]:
    """Analysis 1 inputs: the eligible-day cohort, the fine and class endpoint
    grains, with the partition invariants of hard check 6."""
    days = spark.table(DOSING_DAY_COHORT).toPandas()
    fine = spark.table(ENDPOINTS_FINE).toPandas()
    class_grain = spark.table(ENDPOINTS_CLASS).toPandas()
    assert set(days["dosing_mode"].unique()) <= set(STRATEGIES), days["dosing_mode"].unique()
    assert not days.duplicated(["_userId", "day"]).any(), "a (user, day) appears under more than one strategy"
    # every fine per-user observation's days come from outcome-eligible single-brand days of that subgroup
    eligible = days[days["is_coverage_ok"] & days["is_single_brand"] & (days["insulin_type"] != "unknown")]
    subgroup_days = eligible.groupby(["_userId", "dosing_mode", "insulin_class", "insulin_type", "age_band"]).size().rename("n_days_cohort")
    merged = fine.merge(subgroup_days.reset_index(), how="left", on=["_userId", "dosing_mode", "insulin_class", "insulin_type", "age_band"])
    assert (merged["n_days"] == merged["n_days_cohort"]).all(), "fine-grain day counts disagree with the day cohort"
    check_repool_matches_class_grain(fine, class_grain)
    return {"days": days, "fine": fine, "class_grain": class_grain}


def load_layer2(spark) -> pd.DataFrame:
    """Analysis 2 inputs: the RPT-1001 §8.1 loader (same cohort construction,
    production suffix) joined to the window insulin label — the single brand
    over the branded days of seg1 start .. seg2 end from insulin_type_by_day,
    'multiple' or 'unknown' otherwise (Supplement S1 §3)."""
    wide = load_transition_endpoints(spark)
    spark.createDataFrame(wide[["_userId", "tb_to_ab_seg1_start", "tb_to_ab_seg2_end"]]).createOrReplaceTempView("_rfai_cohort_windows")
    labels = spark.sql(f"""
        SELECT w._userId,
               CASE WHEN COUNT(DISTINCT d.insulin_type) = 0 THEN 'unknown'
                    WHEN COUNT(DISTINCT d.insulin_type) > 1 THEN 'multiple'
                    ELSE MAX(d.insulin_type) END AS window_insulin_type,
               COUNT(d.day) AS branded_days_in_window
        FROM _rfai_cohort_windows w
        LEFT JOIN {INSULIN_TYPE_BY_DAY} d
          ON d._userId = w._userId
         AND CAST(d.day AS DATE) BETWEEN CAST(w.tb_to_ab_seg1_start AS DATE) AND CAST(w.tb_to_ab_seg2_end AS DATE)
        GROUP BY w._userId
    """).toPandas()
    wide = wide.merge(labels, on="_userId", how="left")
    assert wide["window_insulin_type"].notna().all()
    wide["insulin_group"] = wide["window_insulin_type"].map(window_insulin_group)
    return wide


# ---------------------------------------------------------------------------
# Analysis 2 statistics
# ---------------------------------------------------------------------------
def layer2_group_statistics(wide: pd.DataFrame, group_display: str) -> pd.DataFrame:
    """Within one insulin group: the §8.1 paired statistics per endpoint
    (parametric and Hodges-Lehmann), one row per endpoint, plus the TIR
    non-inferiority test row. Distributional statistics withheld under the
    small-subgroup rule."""
    rows = []
    withheld = len(wide) < SMALL_SUBGROUP_USERS
    for col, label in ENDPOINTS:
        if f"{col}_seg1" not in wide.columns:      # derived rates are Analysis 1 quantities
            continue
        seg1, seg2 = wide[f"{col}_seg1"], wide[f"{col}_seg2"]
        s = compute_paired_statistics(seg1, seg2)
        hl = hodges_lehmann_ci(seg1, seg2)
        row = {"insulin_group": group_display, "endpoint": label, "n_users": int((seg1.notna() & seg2.notna()).sum()), "withheld": withheld}
        if not withheld:
            row.update({k: s.get(k) for k in s}); row.update({f"hl_{k}": v for k, v in hl.items()})
        rows.append(row)
    ni = paired_non_inferiority(wide["tir_seg1"], wide["tir_seg2"], margin=NI_MARGIN_TIR)
    rows.append({"insulin_group": group_display, "endpoint": f"TIR non-inferiority (margin {NI_MARGIN_TIR:g} points)",
                 "n_users": ni["n"], "withheld": withheld, **({} if withheld else {f"ni_{k}": v for k, v in ni.items()})})
    return pd.DataFrame(rows)


def layer2_between_groups(wide: pd.DataFrame) -> pd.DataFrame:
    """The between-group contrasts on the paired TIR difference: URAI - RAI
    (primary), Fiasp - RAI, Lyumjev - RAI; Welch interval, no equivalence test."""
    delta = (wide["tir_seg2"] - wide["tir_seg1"]).rename("delta_tir")
    frame = pd.concat([wide[["insulin_group", "window_insulin_type"]], delta], axis=1)
    reference = frame.loc[frame["insulin_group"] == PRIMARY_CONTRAST[1], "delta_tir"]
    rows = []
    for display, level, label in GROUPS:
        if display == PRIMARY_CONTRAST[1]:
            continue
        col = "insulin_group" if level == "class" else "window_insulin_type"
        comparator = frame.loc[frame[col] == label, "delta_tir"]
        r = welch_difference_ci(comparator, reference)
        rows.append({"contrast": f"{display} − {PRIMARY_CONTRAST[1]}", "primary": display == PRIMARY_CONTRAST[0], **r})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tables and figures (PLN §8 numbering) — shells to be aligned with Appendix E
# ---------------------------------------------------------------------------
def build_table_8_1a_population(days: pd.DataFrame) -> pd.DataFrame:
    """Population and cohort flow by dosing strategy (the Phase 0 result 0
    stages, from the staged day cohort)."""
    def stage(name, mask):
        out = []
        for mode in STRATEGIES:
            sub = days[mask & (days["dosing_mode"] == mode)]
            out.append({"stage": name, "dosing_mode": mode, "n_user_days": len(sub), "n_users": sub["_userId"].nunique()})
        return out
    typed = days["insulin_type"] != "unknown"
    labelled = days["insulin_class"].isin(list(CLASS_MEMBERS))
    rows = []
    rows += stage("Eligible dosing days", pd.Series(True, index=days.index))
    rows += stage("Outcome-eligible days (coverage gate)", days["is_coverage_ok"])
    rows += stage("Day carries a confirmed insulin type", days["is_coverage_ok"] & typed)
    rows += stage("Excluded: two-brand (pod-change) days", days["is_coverage_ok"] & ~days["is_single_brand"])
    rows += stage("Excluded: no insulin type on the day", days["is_coverage_ok"] & ~typed)
    rows += stage("Excluded: Apidra days", days["is_coverage_ok"] & days["is_single_brand"] & days["insulin_type"].isin(APPENDIX_BRANDS))
    universe = days["is_coverage_ok"] & days["is_single_brand"] & labelled
    rows += stage("Analysis 1 universe", universe)
    for cls in CLASS_MEMBERS:
        rows += stage(f"of which {cls}", universe & (days["insulin_class"] == cls))
    for brand in sum(CLASS_MEMBERS.values(), []):
        rows += stage(f"of which {brand}", universe & (days["insulin_type"] == brand))
    return pd.DataFrame(rows)


def layer1_between_groups(per_user_by_class: pd.DataFrame, per_user_by_brand: pd.DataFrame) -> pd.DataFrame:
    """Table 8.1e: within each dosing strategy, the difference in per-user
    pooled means between insulin groups (URAI − RAI, Fiasp − RAI, Lyumjev −
    RAI) with Welch's 95% confidence interval, per endpoint. Estimation only."""
    rows = []
    for mode in STRATEGIES:
        reference = per_user_by_class[(per_user_by_class["dosing_mode"] == mode) & (per_user_by_class["insulin_class"] == PRIMARY_CONTRAST[1])]
        comparators = [("URAI", per_user_by_class[(per_user_by_class["dosing_mode"] == mode) & (per_user_by_class["insulin_class"] == "URAI")])]
        comparators += [(b, per_user_by_brand[(per_user_by_brand["dosing_mode"] == mode) & (per_user_by_brand["insulin_type"] == b)]) for b in CLASS_MEMBERS["URAI"]]
        for display, comp in comparators:
            for col, label in ENDPOINTS:
                r = welch_difference_ci(comp[col], reference[col])
                rows.append({"dosing_mode": mode, "contrast": f"{display} − {PRIMARY_CONTRAST[1]}", "endpoint": label,
                             "withheld": min(r["n_a"], r["n_b"]) < SMALL_SUBGROUP_USERS, **r})
    out = pd.DataFrame(rows)
    stat_cols = ["diff", "se", "df", "ci_low", "ci_high", "t_stat", "p_value"]
    out.loc[out["withheld"], stat_cols] = np.nan
    return out


def build_figures(*args, **kwargs):
    raise NotImplementedError(
        "Figures 8.1a–8.1c, 8.2a–8.2c, 8.3a: two dosing-strategy panels of four insulin bars (8.1a), stacked ranges, "
        "raw transitions and paired differences in 4x2 layouts — next pass, aligned with PLN §8 and the RFAI figure style.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_analysis(spark, output_dir: str = None) -> Dict[str, pd.DataFrame]:
    output_dir = output_dir or OUTPUT_DIR
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)          # the run owns its folder: no stale files from an earlier version
    os.makedirs(output_dir)
    print("=" * 60); print("PLN-RFAI-0001: outcomes by dosing strategy and insulin class"); print("=" * 60)

    print("\n1. Analysis 1 inputs (staged day cohort + endpoint grains; hard checks 5, 6)...")
    layer1 = load_layer1(spark)
    print("\n2. Table 8.1a — population and cohort flow by strategy...")
    table_8_1a = build_table_8_1a_population(layer1["days"])
    write_csv(table_8_1a, output_dir, "table_8_1a_population.csv")

    print("\n3. Analysis 1 — per-user pooled outcomes by strategy x insulin group (Tables 8.1b–8.1e)...")
    fine = add_hypo_rates(layer1["fine"])
    per_user_by_class = add_hypo_rates(repool(layer1["fine"], ["_userId", "dosing_mode", "insulin_class"]))
    per_user_by_brand = add_hypo_rates(repool(layer1["fine"], ["_userId", "dosing_mode", "insulin_type"]))
    table_8_1b = summarize_subgroups(per_user_by_class, ["dosing_mode", "insulin_class"])
    table_8_1c = summarize_subgroups(per_user_by_brand, ["dosing_mode", "insulin_type"])
    table_8_1d = summarize_subgroups(per_user_by_class[per_user_by_class["n_days"] >= MIN_DAYS_SENSITIVITY], ["dosing_mode", "insulin_class"])
    pediatric = fine[fine["age_band"].isin(PEDIATRIC_BANDS)]
    table_8_3a = summarize_subgroups(add_hypo_rates(repool(pediatric, ["_userId", "dosing_mode", "insulin_class"])), ["dosing_mode", "insulin_class"])
    for frame, stem in [(table_8_1b, "table_8_1b"), (table_8_1c, "table_8_1c"), (table_8_1d, "table_8_1d"), (table_8_3a, "table_8_3a")]:
        parametric, nonparametric = split_parametric_nonparametric(frame)
        write_csv(parametric, output_dir, f"{stem}_parametric.csv")
        write_csv(nonparametric, output_dir, f"{stem}_nonparametric.csv")
    table_8_1e = layer1_between_groups(per_user_by_class, per_user_by_brand)
    write_csv(table_8_1e, output_dir, "table_8_1e_between_group_differences.csv")

    print("\n4. Analysis 2 inputs (§8.1 loader + window insulin label)...")
    wide = load_layer2(spark)
    print("\n5. Hard check 1 — all-users block reproduces the production Table 8.1a files...")
    check_reproduces_production_8_1(wide)
    print("\n6. Analysis 2 — within-group paired statistics and TIR non-inferiority (Tables 8.2a, 8.2b, 8.2d)...")
    within = []
    for display, level, label in GROUPS:
        col = "insulin_group" if level == "class" else "window_insulin_type"
        within.append(layer2_group_statistics(wide[wide[col] == label], display))
    within.append(layer2_group_statistics(wide, "All users"))
    table_8_2a = pd.concat(within, ignore_index=True)
    write_csv(table_8_2a, output_dir, "table_8_2a_within_group_paired.csv")
    print("\n7. Analysis 2 — between-group contrasts on the paired TIR difference (Table 8.2c)...")
    table_8_2c = layer2_between_groups(wide)
    write_csv(table_8_2c, output_dir, "table_8_2c_between_group_contrasts.csv")
    flow_8_2 = wide.groupby(["insulin_group", "window_insulin_type"]).size().rename("n_users").reset_index()
    write_csv(flow_8_2, output_dir, "table_8_2_group_counts.csv")

    print("\n8. Figures...")
    try:
        build_figures(per_user_by_class, per_user_by_brand, wide, output_dir)
    except NotImplementedError as exc:
        print(f"  skipped: {exc}")

    print("\n" + "=" * 60); print("Analysis complete (scaffold run)"); print("=" * 60)
    return {"table_8_1a": table_8_1a, "table_8_1b": table_8_1b, "table_8_1c": table_8_1c, "table_8_1d": table_8_1d,
            "table_8_1e": table_8_1e, "table_8_3a": table_8_3a, "table_8_2a": table_8_2a, "table_8_2c": table_8_2c, "group_counts": flow_8_2,
            "wide": wide, "per_user_by_class": per_user_by_class}


def run_in_databricks(spark):
    return run_analysis(spark)


if __name__ == "__main__":
    run_in_databricks(spark)  # type: ignore[name-defined]  # Databricks notebook global
