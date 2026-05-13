"""
=============================================================================
Analysis 8.6: Socioeconomic Subgroup Analysis
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Two paths:

1. Export (`--mode export`, the Databricks-side path): provide JAEB-linked
   glycemic endpoints to the partner team for socioeconomic stratification.

2. Figures (`--mode figures`, runs locally on the partner's returned summary
   stats CSV): render subgroup boxes (median + IQR) for TIR, TBR, and 14-day
   hypo-event rate across Race/Ethnicity, Income, Education, Insurance, and
   helpStartLoop.

Population (export side): users with valid stable autobolus segments (≥14
days of 100% autobolus usage, beginning at least 30 days after first
autobolus use), CBG coverage ≥ MIN_CBG_COUNT, age ≥ MIN_AGE (or DOB
unknown), with linked JAEB demographic data.

Inputs (export):
- dev.fda_510k_rwd.glycemic_endpoints_stable_autobolus
- dev.fda_510k_rwd.stable_autobolus_segments  (age eligibility)
- dev.default.jaeb_upload_to_userid + dev.default.bddp_sample_all_2

Outputs:
- CSV (export):  outputs/analysis_8_6/glycemic_endpoints_by_jaeb_id.csv
- PNGs (figures):
    outputs/analysis_8_6/figure_8_6a_tir.png
    outputs/analysis_8_6/figure_8_6b_tbr.png
    outputs/analysis_8_6/figure_8_6c_hypo_rate.png
=============================================================================
"""

import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd

from utils import MIN_CBG_COUNT
from utils.constants import COLORS_PRIMARY, COLORS_SECONDARY, FONT
from utils.data_loading import MIN_AGE

OUTPUT_DIR = "outputs/analysis_8_6"

ENDPOINT_COLS = [
    "tir", "tbr", "tbr_very_low", "tar", "tar_very_high",
    "mean_glucose", "cv", "hypo_events", "cbg_count",
]

# Subgroup figures (rendered from the partner's returned summary CSV).
# Each entry: (glucVar key in CSV, y-axis label, output filename).
METRICS = [
    ("tir",                "TIR (%)",            "figure_8_6a_tir.png"),
    ("tbr",                "TBR (%)",            "figure_8_6b_tbr.png"),
    ("hypoEventRate14Day", "Hypo events / 14d",  "figure_8_6c_hypo_rate.png"),
]

# Left-to-right panel order. Second element is the display title.
SUBGROUP_ORDER = [
    ("Race/Ethnicity", "Race / Ethnicity"),
    ("Income",         "Income"),
    ("Education",      "Education"),
    ("Insurance",      "Insurance"),
    ("helpStartLoop",  "Help Starting Loop"),
]


def load_data(spark) -> pd.DataFrame:
    """
    Load glycemic endpoints for stable autobolus users and link to JAEB PtID.
    """
    # --- Glycemic endpoints ---
    endpoints = spark.table("dev.fda_510k_rwd.glycemic_endpoints_stable_autobolus").toPandas()

    for col in endpoints.select_dtypes(include=["object"]).columns:
        if col not in ("_userId", "segment"):
            endpoints[col] = pd.to_numeric(endpoints[col], errors="coerce")

    # CBG coverage filter (70% of 14-day period)
    endpoints = endpoints.loc[endpoints["cbg_count"] >= MIN_CBG_COUNT].copy()
    print(f"  Users after CBG coverage filter: {len(endpoints)}")

    # Age eligibility (Loop autobolus indication: age ≥6 at segment start;
    # users with unknown DOB are kept).
    age_eligible = spark.sql(f"""
        --begin-sql
        SELECT _userId
        FROM dev.fda_510k_rwd.stable_autobolus_segments
        WHERE age_years >= {MIN_AGE} OR age_years IS NULL
    ;
    """).toPandas()
    pre_age = endpoints["_userId"].nunique()
    endpoints = endpoints.merge(age_eligible, on="_userId", how="inner")
    print(f"  Users after age filter (≥{MIN_AGE}): {endpoints['_userId'].nunique()}/{pre_age}")

    # --- JAEB PtID linkage ---
    jaeb_map = spark.sql("""
        --begin-sql
        SELECT DISTINCT j.PtID, b._userId
        FROM dev.default.jaeb_upload_to_userid j
        INNER JOIN dev.default.bddp_sample_all_2 b
            ON j.uploadID = b.uploadID
    ;
    """).toPandas()

    print(f"  JAEB-linked users in mapping table: {jaeb_map['_userId'].nunique()}")

    # --- Join ---
    merged = endpoints.merge(jaeb_map, on="_userId", how="inner")
    merged = merged.drop_duplicates(subset="PtID")
    print(f"  Users after JAEB linkage: {len(merged)}")

    return merged


def run_analysis(spark, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.6: Socioeconomic Subgroup — Glycemic Endpoint Export")
    print("=" * 60)

    print("\n1. Loading data and linking JAEB PtID...")
    df = load_data(spark)

    print(f"\n2. Exporting {len(df)} rows...")
    out_cols = ["PtID"] + ENDPOINT_COLS
    out_df = df[out_cols].sort_values("PtID").reset_index(drop=True)

    out_path = f"{output_dir}/glycemic_endpoints_by_jaeb_id.csv"
    out_df.to_csv(out_path, index=False)
    print(f"  Saved: {out_path}")

    print("\n  Summary statistics:")
    print(out_df[ENDPOINT_COLS].describe().round(2).to_string())

    print("\n" + "=" * 60)
    print("Analysis 8.6 Complete!")
    print(f"Outputs saved to: {output_dir}/")
    print("=" * 60)

    return out_df


def run_in_databricks(spark):
    return run_analysis(spark)


# =============================================================================
# Figures from partner-returned summary statistics
# =============================================================================

def load_subgroup_stats(csv_path: str) -> pd.DataFrame:
    """Load the partner's summary CSV (SubGroup, SubGroupCategory, glucVar,
    median, q1, q3). SubGroupCategory values come prefixed (e.g. "1.White",
    "2.Non-White"); the prefix fixes left/right ordering, the suffix is the
    display label."""
    df = pd.read_csv(csv_path)
    df["display_label"] = df["SubGroupCategory"].str.split(".", n=1).str[1]
    return df


def create_figure_8_6(
    stats_df: pd.DataFrame,
    metric_key: str,
    ylabel: str,
    fname: str,
    output_dir: str,
    letter: str,
):
    """Render one metric across all 5 subgroups (each as a 2-box panel)."""
    fig, axes = plt.subplots(1, len(SUBGROUP_ORDER), figsize=(15, 5), sharey=True)

    for ax, (subgroup, display) in zip(axes, SUBGROUP_ORDER):
        rows = (
            stats_df[(stats_df["SubGroup"] == subgroup)
                     & (stats_df["glucVar"] == metric_key)]
            .sort_values("SubGroupCategory")
        )
        if len(rows) != 2:
            ax.text(0.5, 0.5, f"Expected 2 levels,\ngot {len(rows)}",
                    ha="center", va="center", fontsize=FONT["annotation"],
                    transform=ax.transAxes)
            ax.set_title(display, fontsize=FONT["title"], fontweight="bold")
            continue

        # Whiskers collapsed to q1/q3 — the CSV doesn't carry min/max, so the
        # figure shows IQR + median only.
        boxes = [
            {"med": r["median"], "q1": r["q1"], "q3": r["q3"],
             "whislo": r["q1"], "whishi": r["q3"], "fliers": []}
            for _, r in rows.iterrows()
        ]
        bp = ax.bxp(boxes, positions=[0, 1], widths=0.5,
                    patch_artist=True, showcaps=False, showfliers=False)
        bp["boxes"][0].set(facecolor=COLORS_PRIMARY, alpha=0.7)
        bp["boxes"][1].set(facecolor=COLORS_SECONDARY, alpha=0.7)

        ax.set_xticks([0, 1])
        ax.set_xticklabels(rows["display_label"].tolist(),
                           fontsize=FONT["tick"], rotation=15, ha="right")
        ax.tick_params(axis="y", labelsize=FONT["tick"])
        ax.set_title(display, fontsize=FONT["title"], fontweight="bold")
        ax.grid(axis="y", alpha=0.3)

    axes[0].set_ylabel(ylabel, fontsize=FONT["axis_label"])
    plt.suptitle(f"Figure 8.6{letter}: {ylabel} by Subgroup",
                 fontsize=FONT["suptitle"], fontweight="bold", y=1.02)
    plt.tight_layout()

    path = os.path.join(output_dir, fname)
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def render_subgroup_figures(csv_path: str, output_dir: str = OUTPUT_DIR):
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("Analysis 8.6: Subgroup Figures from Partner Summary Stats")
    print("=" * 60)
    print(f"\nInput CSV: {csv_path}")

    stats_df = load_subgroup_stats(csv_path)

    print(f"\nRendering {len(METRICS)} figures...")
    for letter, (key, ylabel, fname) in zip("abc", METRICS):
        create_figure_8_6(stats_df, key, ylabel, fname, output_dir, letter)

    print("\n" + "=" * 60)
    print(f"Outputs saved to: {output_dir}/")
    print("=" * 60)


# =============================================================================
# CLI dispatcher
# =============================================================================

def _parse_args():
    parser = argparse.ArgumentParser(description="Analysis 8.6")
    parser.add_argument("--mode", choices=("export", "figures"), default="export",
                        help="export: Databricks JAEB-endpoint CSV. "
                             "figures: render subgroup figures from partner summary CSV.")
    parser.add_argument("--input-csv", default=None,
                        help="Path to partner summary CSV (required for --mode figures).")
    parser.add_argument("--output-dir", default=OUTPUT_DIR,
                        help=f"Output directory (default: {OUTPUT_DIR}).")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.mode == "figures":
        if not args.input_csv:
            raise SystemExit("--mode figures requires --input-csv PATH")
        render_subgroup_figures(args.input_csv, args.output_dir)
    else:
        run_in_databricks(spark)  # type: ignore[name-defined]
