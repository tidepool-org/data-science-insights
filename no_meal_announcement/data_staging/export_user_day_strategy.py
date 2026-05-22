"""
Per-user-day Loop delivery-strategy label (PLN-1008 §7.3).

Reads `dev.fda_510k_rwd.loop_recommendations` (built by PLN-1001's
`FDA_real_world_data/data_staging/export_loop_recommendations.py`) and
applies the ≥3-autobolus rule at the per-day grain:

    autobolus  : GREATEST(dd_autobolus_count, hk_autobolus_count) >= 3
    temp_basal : autobolus rule fails AND
                 GREATEST(dd_temp_basal_count, hk_temp_basal_count) >= 1
    ambiguous  : neither rule fires; per §7.3 these days are excluded from
                 Analysis 2 (still kept in Analyses 1 and 3 by default).

The threshold of 3 mirrors `FDA_real_world_data/data_staging/export_valid_transition_segments.py`
(lines 43-62) and is exposed as `min_autobolus_count` for sensitivity sweeps.

Output table columns: `_userId`, `day`, `delivery_strategy`,
`dd_autobolus_count`, `hk_autobolus_count`, `dd_temp_basal_count`,
`hk_temp_basal_count`, `is_ambiguous`.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    loop_recommendations_table: str = "dev.fda_510k_rwd.loop_recommendations",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_strategy",
    min_autobolus_count: int = 3,
    min_temp_basal_count: int = 1,
) -> None:
    """Assign per-day delivery strategy from existing loop_recommendations table.

    Args:
        spark: SparkSession.
        loop_recommendations_table: PLN-1001 per-day recommendation counts.
        output_table: Destination Unity Catalog table.
        min_autobolus_count: Threshold for the autobolus rule (default 3).
        min_temp_basal_count: Threshold for the temp_basal rule (default 1).

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
