"""
=============================================================================
Analysis 8.6: Socioeconomic Subgroup Analysis — Glycemic Endpoint Export
FDA 510(k) Submission: Loop Autobolus Feature
=============================================================================

Objective: Provide glycemic endpoint data with JAEB PtID for downstream
socioeconomic subgroup analysis by the partner team.

Population: Users with valid stable autobolus segments (≥14 days of 100%
autobolus usage, beginning at least 30 days after first autobolus use) who
have linked JAEB demographic data.

Inputs:
- dev.fda_510k_rwd.glycemic_endpoints_stable_autobolus  (_userId, segment, metrics)
- dev.default.jaeb_upload_to_userid                      (userid, PtID, uploadID)
- dev.default.bddp_sample_all                            (_userId, uploadID)

Outputs:
- CSV: outputs/analysis_8_6/glycemic_endpoints_by_jaeb_id.csv
  Columns: PtID, tir, tbr, tbr_very_low, tar, tar_very_high,
           mean_glucose, cv, hypo_events, cbg_count
=============================================================================
"""

import pandas as pd
import os

OUTPUT_DIR = "outputs/analysis_8_6"

ENDPOINT_COLS = [
    "tir", "tbr", "tbr_very_low", "tar", "tar_very_high",
    "mean_glucose", "cv", "hypo_events", "cbg_count",
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
    endpoints = endpoints.loc[endpoints["cbg_count"] >= 14 * 288 * 0.7].copy()
    print(f"  Users after CBG coverage filter: {len(endpoints)}")

    # --- JAEB PtID linkage ---
    jaeb_map = spark.sql("""
        SELECT DISTINCT j.PtID, b._userId
        FROM dev.default.jaeb_upload_to_userid j
        INNER JOIN dev.default.bddp_sample_all b
            ON j.uploadID = b.uploadID
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


run_in_databricks(spark) # type: ignore[name-defined]
