"""Select the clean users of an export bundle and build the hand-off (local).

"Clean" means every criterion below holds, each measured from the bundle
itself (cohort_windows.csv and each user's decision_times.csv), so the list is
reproducible from the files a collaborator receives:

  1. exported (core tier: settings inside the window, Loop's decisions present);
  2. decision coverage: at least MIN_DECISIONS_PER_DAY loop decisions per window day
     (the cadence, disruption, and strategy sidecars are complete);
  3. CGM reproduces the FDA pipeline's count within CGM_RATIO_BAND (a stream far
     above 1 is a second sensor or duplicated uploads; far below, lost samples);
  4. no more than MAX_INACTIVE_ROW_PCT of bolus, basal, or food rows flagged
     inactive (the ETL drops inactive dose and carb records);
  5. automatic boluses at least MIN_AUTO_PER_RECOMMENDATION of Loop's bolus
     recommendations (the automated label reached the surviving records);
  6. the two periods are what the cohort says they are, by the DELIVERED
     automatic boluses in the export, counted with the cohort's own day rule
     (an autobolus day has at least AUTOBOLUS_DAY_MIN automatic boluses): at
     most MAX_AB_DAYS_IN_TB_PERIOD such days in the temporary-basal period and
     at least MIN_AB_DAYS_IN_AB_PERIOD in the automated-bolus period. Guards
     against a "transition" that is only the start of the plugin-route data,
     where Loop was dosing automated boluses throughout; the ETL's inferred
     per-cycle mode is reported alongside but not used;
  7. at least MIN_CARBS carb entries and a carb visibility lag p90 under
     MAX_CARB_LAG_P90_MIN minutes.

Writes clean_users.csv (every exported user, each criterion as a column, the
verdict and the reasons) into the bundle and a hand-off folder + zip holding
only the clean users' directories, the bundle-level files, and the README.

Run (house interpreter):
    /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \\
        loopeval_export/select_clean_users.py <run_folder> [--out <handoff folder>]
"""

import argparse
import csv
import json
import os
import shutil
import sys
import tempfile

import pandas as pd

# =============================================================================
# Criteria (constants; each appears as a column in clean_users.csv)
# =============================================================================
MIN_DECISIONS_PER_DAY = 200          # of ~288 five-minute cycles
CGM_RATIO_BAND = (0.95, 1.10)        # exported samples / pipeline count over the 28 days
MAX_INACTIVE_ROW_PCT = 10.0          # bolus, basal, food rows flagged inactive
MIN_AUTO_PER_RECOMMENDATION = 0.5    # automatic boluses / loop bolus recommendations
AUTOBOLUS_DAY_MIN = 3                # the cohort's day rule: >= 3 automatic boluses = an autobolus day
MAX_AB_DAYS_IN_TB_PERIOD = 2         # the validity box allows at most 20% of 14 days
MIN_AB_DAYS_IN_AB_PERIOD = 12        # and at least 80%
MIN_CARBS = 20
MAX_CARB_LAG_P90_MIN = 240.0


def period_mode_shares(user_dir, manifest):
    """(share of temp-mode cycles in the temporary-basal period, share of bolus-mode cycles in the automated-bolus period)."""
    d = manifest["segment_dates"]
    dec = pd.read_csv(os.path.join(user_dir, "decision_times.csv"))
    if dec.empty:
        return 0.0, 0.0
    dec["t"] = pd.to_datetime(dec["t"], utc=True)
    def share(start, end, mode):
        lo, hi = pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)
        part = dec[(dec.t >= lo) & (dec.t < hi)]
        return float((part["mode"] == mode).mean()) if len(part) else 0.0
    return share(d["seg1_start"], d["seg1_end"], "temp"), share(d["seg2_start"], d["seg2_end"], "bolus")


def autobolus_days(user_dir, manifest):
    """(autobolus days in the temporary-basal period, in the automated-bolus period) from the export's own labelled boluses."""
    d = manifest["segment_dates"]
    doses = pd.DataFrame(json.load(open(os.path.join(user_dir, "doses.json"))))
    doses["t"] = pd.to_datetime(doses["startDate"], utc=True)
    auto = doses[(doses.deliveryType == "bolus") & (doses.automatic == True)]  # noqa: E712
    per_day = auto.groupby(auto.t.dt.floor("D")).size()
    def count(start, end):
        lo, hi = pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)
        days = per_day[(per_day.index >= lo) & (per_day.index < hi)]
        return int((days >= AUTOBOLUS_DAY_MIN).sum())
    return count(d["seg1_start"], d["seg1_end"]), count(d["seg2_start"], d["seg2_end"])


def evaluate(run_folder):
    w = pd.read_csv(os.path.join(run_folder, "cohort_windows.csv"))
    exported = w[w.export_status == "ok"].copy()
    rows = []
    for r in exported.itertuples():
        user_dir = os.path.join(run_folder, r.user_alias)
        manifest = json.load(open(os.path.join(user_dir, "_export_manifest.json")))
        window_days = (pd.Timestamp(r.export_end) - pd.Timestamp(r.export_start)).days + 1
        temp_share, bolus_share = period_mode_shares(user_dir, manifest)
        ab_days_tb, ab_days_ab = autobolus_days(user_dir, manifest)
        recs = float(r.n_bolus_recommendations or 0)
        checks = {
            "core_tier": r.tier == "core",
            "decision_coverage": (float(r.n_loop_decisions) / window_days) >= MIN_DECISIONS_PER_DAY,
            "cgm_ratio_in_band": CGM_RATIO_BAND[0] <= float(r.cgm_ratio_vs_pipeline) <= CGM_RATIO_BAND[1],
            "inactive_rows_low": max(float(r.pct_bolus_rows_inactive or 0), float(r.pct_basal_rows_inactive or 0),
                                     float(r.pct_food_rows_inactive or 0)) <= MAX_INACTIVE_ROW_PCT,
            "automatic_boluses_labelled": recs == 0 or (float(r.n_bolus_automatic) / recs) >= MIN_AUTO_PER_RECOMMENDATION,
            "periods_as_classified": ab_days_tb <= MAX_AB_DAYS_IN_TB_PERIOD and ab_days_ab >= MIN_AB_DAYS_IN_AB_PERIOD,
            "carbs_usable": float(r.n_carbs) >= MIN_CARBS and float(r.carb_visibility_lag_p90_min or 0) <= MAX_CARB_LAG_P90_MIN,
        }
        rows.append(dict(user_alias=r.user_alias, tier=r.tier, insulin_type_preset=r.insulin_type_preset,
                         decisions_per_day=round(float(r.n_loop_decisions) / window_days, 1),
                         cgm_ratio_vs_pipeline=r.cgm_ratio_vs_pipeline,
                         max_inactive_row_pct=round(max(float(r.pct_bolus_rows_inactive or 0), float(r.pct_basal_rows_inactive or 0), float(r.pct_food_rows_inactive or 0)), 2),
                         auto_per_recommendation=round(float(r.n_bolus_automatic) / recs, 2) if recs else None,
                         autobolus_days_in_tb_period=ab_days_tb, autobolus_days_in_ab_period=ab_days_ab,
                         temp_mode_share_tb_period=round(temp_share, 3), bolus_mode_share_ab_period=round(bolus_share, 3),
                         n_carbs=int(r.n_carbs), carb_lag_p90_min=round(float(r.carb_visibility_lag_p90_min or 0), 1),
                         **checks, clean=all(checks.values()),
                         reasons="; ".join(k for k, v in checks.items() if not v)))
    return pd.DataFrame(rows)


def build_handoff(run_folder, verdicts, out_dir):
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)
    clean = verdicts[verdicts["clean"]]
    for alias in clean.user_alias:
        shutil.copytree(os.path.join(run_folder, alias), os.path.join(out_dir, alias))
    for name in ("README.md", "cohort_windows.csv", "run_summary.csv", "decode_coverage.csv", "bolus_diagnostic.csv", "run_manifest.json", "clean_users.csv"):
        src = os.path.join(run_folder, name)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(out_dir, name))
    local_zip = shutil.make_archive(os.path.join(tempfile.gettempdir(), os.path.basename(out_dir)), "zip", out_dir)
    zip_path = out_dir + ".zip"
    shutil.move(local_zip, zip_path)
    return zip_path


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("run_folder"); parser.add_argument("--out", default=None)
    a = parser.parse_args(argv)
    verdicts = evaluate(a.run_folder)
    verdicts.sort_values(["clean", "user_alias"], ascending=[False, True]).to_csv(os.path.join(a.run_folder, "clean_users.csv"), index=False)
    n_clean = int(verdicts["clean"].sum())
    print(f"{n_clean} clean of {len(verdicts)} exported")
    for reason, n in verdicts[~verdicts["clean"]]["reasons"].str.split("; ").explode().value_counts().items():
        print(f"  excluded by {reason}: {n}")
    out_dir = a.out or os.path.join(os.path.dirname(a.run_folder.rstrip("/")), "handoff_" + os.path.basename(a.run_folder.rstrip("/")))
    zip_path = build_handoff(a.run_folder, verdicts, out_dir)
    print(f"hand-off: {out_dir} ({n_clean} users) and {zip_path} ({os.path.getsize(zip_path) / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
