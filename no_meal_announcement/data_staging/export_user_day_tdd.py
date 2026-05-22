"""
Total daily insulin delivered (TDD) per user-day.

For each (`_userId`, user-local `day`), compute:
    - `tdd_basal_u`  = sum of basal insulin delivered over the calendar day
    - `tdd_bolus_u`  = sum of `bolus.normal` insulin delivered (includes
                       autoboluses and manual boluses; matches PLN-1001
                       TDD convention)
    - `tdd_u`        = `tdd_basal_u + tdd_bolus_u`

Basal delivery is reconstructed from BDDP `type='basal'` records (rate ×
duration); refer to PLN-1001 implementation in
`FDA_real_world_data/data_staging/` for the basal-delivered helper.

Output table columns: `_userId`, `day`, `tdd_basal_u`, `tdd_bolus_u`, `tdd_u`.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    input_table: str = "dev.default.bddp_sample_all_2",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_tdd",
    cohort_table: Optional[str] = None,
) -> None:
    """Compute basal + bolus insulin per user-day.

    Args:
        spark: SparkSession.
        input_table: BDDP source table.
        output_table: Destination Unity Catalog table.
        cohort_table: Optional cohort filter table.

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
