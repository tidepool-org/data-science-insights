"""Distribution of ISF values from pump settings during valid transitions.

For every user in `valid_transition_segments`, pull all `pumpSettings` rows
with `time_string` falling inside their TB→AB window
(`tb_to_ab_seg1_start` … `tb_to_ab_seg2_end`), parse the `insulinSensitivities`
JSON, and plot the distribution of every schedule entry's `amount`
(converted from mmol/L → mg/dL/U).
"""

import json

import matplotlib.pyplot as plt
import pandas as pd

MMOL_TO_MGDL = 18.016


def get_pump_settings_for_transitions(spark):
    return spark.sql("""
    --begin-sql
    WITH segments AS (
        SELECT
            _userId,
            tb_to_ab_seg1_start,
            tb_to_ab_seg2_end,
            segment_rank
        FROM dev.fda_510k_rwd.valid_transition_segments
    ),
    pump_settings AS (
        SELECT
            _userId,
            time_string,
            insulinSensitivities
        FROM dev.default.bddp_sample_all_2
        WHERE type = 'pumpSettings'
    )
    SELECT
        ps._userId,
        ps.time_string,
        ps.insulinSensitivities,
        seg.tb_to_ab_seg1_start AS segment_start,
        seg.tb_to_ab_seg2_end AS segment_end,
        seg.segment_rank
    FROM pump_settings ps
    INNER JOIN segments seg
        ON ps._userId = seg._userId
        AND TRY_CAST(ps.time_string AS DATE)
            BETWEEN seg.tb_to_ab_seg1_start AND seg.tb_to_ab_seg2_end
    ;
    """).toPandas()


def extract_isf_values(df):
    """Explode every (user, settings_row) into one row per schedule entry.

    `insulinSensitivities` is a JSON dict of schedule_name -> [{start, amount}].
    Amount is mmol/L; convert to mg/dL/U.
    """
    rows = []
    for _, r in df.iterrows():
        raw = r["insulinSensitivities"]
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(data, dict):
            continue
        for schedule_name, entries in data.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                amount = entry.get("amount")
                if amount is None:
                    continue
                rows.append({
                    "_userId": r["_userId"],
                    "time_string": r["time_string"],
                    "segment_rank": r["segment_rank"],
                    "schedule_name": schedule_name,
                    "start": entry.get("start"),
                    "isf_mgdl_per_u": round(float(amount) * MMOL_TO_MGDL, 2),
                })
    return pd.DataFrame(rows)


def summarize(isf_df):
    s = isf_df["isf_mgdl_per_u"]
    print(f"Pump settings rows (after explode): {len(isf_df):,}")
    print(f"Distinct users: {isf_df['_userId'].nunique():,}")
    print(f"\nISF (mg/dL/U) distribution:")
    print(s.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]).to_string())


def plot_distribution(isf_df, output_path=None):
    s = isf_df["isf_mgdl_per_u"]

    _, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].hist(s, bins=60, edgecolor="black", color="#607cff")
    axes[0].axvline(s.median(), color="#241144", ls="--", lw=2,
                    label=f"median = {s.median():.0f}")
    axes[0].axvline(s.mean(), color="#8B0000", ls="-", lw=2,
                    label=f"mean = {s.mean():.0f}")
    axes[0].set_xlabel("ISF (mg/dL per U)")
    axes[0].set_ylabel("Number of schedule entries")
    axes[0].set_title("ISF — all schedule entries from pump settings\nduring valid TB→AB transitions")
    axes[0].legend()
    axes[0].grid(axis="y", alpha=0.3)

    user_median = isf_df.groupby("_userId")["isf_mgdl_per_u"].median()
    axes[1].hist(user_median, bins=40, edgecolor="black", color="#4f59be")
    axes[1].axvline(user_median.median(), color="#241144", ls="--", lw=2,
                    label=f"median = {user_median.median():.0f}")
    axes[1].set_xlabel("Per-user median ISF (mg/dL per U)")
    axes[1].set_ylabel("Number of users")
    axes[1].set_title(f"Per-user median ISF (n={len(user_median):,} users)")
    axes[1].legend()
    axes[1].grid(axis="y", alpha=0.3)

    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Saved: {output_path}")
    else:
        plt.show()


def run(spark, output_path=None):
    print("Pulling pump settings for valid-transition windows...")
    raw_df = get_pump_settings_for_transitions(spark)
    print(f"  pumpSettings rows in window: {len(raw_df):,}")

    print("\nExtracting ISF schedule entries...")
    isf_df = extract_isf_values(raw_df)

    summarize(isf_df)
    plot_distribution(isf_df, output_path=output_path)
    return isf_df


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841
    run(spark)
