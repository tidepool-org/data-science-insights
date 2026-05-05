"""Export per-scenario TIR (seg1 = temp basal, seg2 = autobolus) for every
exported simulation user, keyed on the anonymized rwd_user_id.

Inputs:
  - simulation/data/scenarios/user_id_mapping.csv  (rwd_user_id, _userId, target_day)
  - dev.fda_510k_rwd.glycemic_endpoints_transition (long: _userId, segment, tir, cbg_count, ...)

Output: simulation/data/scenarios/scenario_tir.csv
  rwd_user_id, _userId, target_day, tir_seg1, tir_seg2,
  cbg_count_seg1, cbg_count_seg2

Matches each exported user's segment_rank=1 window (target_day =
tb_to_ab_seg1_start) to its TIR row in glycemic_endpoints_transition.
TIR is the same metric used in analysis_8-1 (% time 70-180 mg/dL).
A scenario whose segment has no row in the endpoints table — e.g. it
was filtered out upstream — keeps its rwd_user_id row but TIR is NaN.
"""

import argparse
import os

import pandas as pd


CATALOG = "dev.fda_510k_rwd"
DEFAULT_SCENARIOS_DIR = (
    "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights"
    "/FDA_real_world_data/simulation/data/scenarios"
)
DEFAULT_ENDPOINTS_TABLE = f"{CATALOG}.glycemic_endpoints_transition"


def run(
    spark,
    scenarios_dir=DEFAULT_SCENARIOS_DIR,
    endpoints_table=DEFAULT_ENDPOINTS_TABLE,
):
    mapping_path = os.path.join(scenarios_dir, "user_id_mapping.csv")
    if not os.path.exists(mapping_path):
        raise FileNotFoundError(
            f"{mapping_path} not found — run build_scenario_json.py first."
        )

    mapping = pd.read_csv(mapping_path, dtype={"_userId": str, "rwd_user_id": str})
    mapping["target_day"] = pd.to_datetime(mapping["target_day"]).dt.date

    spark.createDataFrame(
        mapping[["_userId", "target_day"]]
    ).createOrReplaceTempView("_scenario_users")

    # Pivot the long endpoints table to one row per (_userId, target_day) with
    # seg1 / seg2 columns side-by-side. Inner-join restricts to rows that
    # match an exported scenario; we left-merge back in pandas so users with
    # no endpoint row still appear with NaN TIR.
    tir_df = spark.sql(f"""
    --begin-sql
    WITH scenario_users AS (
        SELECT
            _userId,
            target_day
        FROM _scenario_users
    )
    SELECT
        e._userId,
        e.tb_to_ab_seg1_start AS target_day,
        MAX(CASE WHEN e.segment = 'tb_to_ab_seg1' THEN TRY_CAST(e.tir AS DOUBLE) END) AS tir_seg1,
        MAX(CASE WHEN e.segment = 'tb_to_ab_seg2' THEN TRY_CAST(e.tir AS DOUBLE) END) AS tir_seg2,
        MAX(CASE WHEN e.segment = 'tb_to_ab_seg1' THEN TRY_CAST(e.cbg_count AS BIGINT) END) AS cbg_count_seg1,
        MAX(CASE WHEN e.segment = 'tb_to_ab_seg2' THEN TRY_CAST(e.cbg_count AS BIGINT) END) AS cbg_count_seg2
    FROM {endpoints_table} e
    INNER JOIN scenario_users s
        ON e._userId = s._userId
        AND e.tb_to_ab_seg1_start = s.target_day
    GROUP BY e._userId, e.tb_to_ab_seg1_start
    ;
    """).toPandas()
    tir_df["target_day"] = pd.to_datetime(tir_df["target_day"]).dt.date

    out = mapping.merge(tir_df, on=["_userId", "target_day"], how="left")
    out = out.sort_values("rwd_user_id").reset_index(drop=True)

    out_path = os.path.join(scenarios_dir, "scenario_tir.csv")
    out.to_csv(out_path, index=False)

    matched = out["tir_seg1"].notna().sum()
    print(
        f"Wrote {len(out)} rows to {out_path} "
        f"({matched} with TIR; {len(out) - matched} unmatched)."
    )
    return out


if __name__ == "__main__":
    spark = spark  # type: ignore[name-defined]  # noqa: F841

    _parser = argparse.ArgumentParser()
    _parser.add_argument("--scenarios_dir", default=DEFAULT_SCENARIOS_DIR)
    _parser.add_argument("--endpoints_table", default=DEFAULT_ENDPOINTS_TABLE)
    _args, _ = _parser.parse_known_args()

    run(
        spark,
        scenarios_dir=_args.scenarios_dir,
        endpoints_table=_args.endpoints_table,
    )
