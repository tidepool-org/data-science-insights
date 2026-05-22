"""
Per-user-day CGM slice with coverage threshold for PLN-1008.

For each (`_userId`, user-local `day`):
    - Slice CBG readings to the calendar day in the user's local timezone
      (shift UTC via BDDP `timezoneOffset`).
    - Compute `cbg_count` and `coverage_pct = cbg_count / 288`.
    - Drop days with coverage_pct < 0.70 (PLN-1008 §7.1).

Dedup CBG records by latest `created_timestamp` per (userId, deviceTime) —
same pattern as the PLN-1001 CBG export.

Output schema is BDDP-compatible (one row per surviving CGM reading) so
that downstream `compute_nma_glycemic_endpoints` can pass it to
`FDA_real_world_data.data_staging.compute_glycemic_endpoints` with
`group_cols=["_userId","day"]`.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    input_table: str = "dev.default.bddp_sample_all_2",
    output_table: str = "dev.fda_510k_rwd.nma_cbg",
    cohort_table: Optional[str] = None,
    min_coverage_pct: float = 0.70,
) -> None:
    """Slice CBG to user-day grain and enforce coverage threshold.

    Args:
        spark: SparkSession.
        input_table: BDDP source table.
        output_table: Destination table (BDDP-compatible CBG rows).
        cohort_table: Optional cohort filter table.
        min_coverage_pct: Minimum fraction of 288 5-min slots covered
                          for a day to be retained (default 0.70).

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
