"""
Per-user-day glycemic endpoints (thin wrapper over FDA implementation).

Imports and calls `FDA_real_world_data.data_staging.compute_glycemic_endpoints.compute_glycemic_endpoints`
with `group_cols=["_userId","day"]`. Outputs one row per user-day with:
    - `pct_time_lt_54`, `pct_time_lt_70`, `pct_time_70_180`,
      `pct_time_gt_180`, `pct_time_gt_250`
    - `mean_glucose_mgdl`
    - `cv_pct`
    - `hypo_events` (3 consecutive <54 readings ending at 3 consecutive ≥70;
      identical definition to PLN-1001)

No new logic — this file exists so the PLN-1008 staging DAG references its
own module name and the wrapper is the integration point if endpoint
definitions ever diverge from PLN-1001.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    input_table: str = "dev.fda_510k_rwd.nma_cbg",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_glycemic_endpoints",
) -> None:
    """Compute per-user-day glycemic endpoints by delegating to FDA helper.

    Args:
        spark: SparkSession.
        input_table: NMA-filtered CBG table from `export_nma_cbg`.
        output_table: Destination Unity Catalog table.

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
