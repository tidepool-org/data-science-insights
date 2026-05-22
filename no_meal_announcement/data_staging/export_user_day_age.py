"""
Per-user-day age and pediatric flag (PLN-1008 §7.6).

For each (`_userId`, user-local `day`):
    - `age_years`     : floor((day_midnight - dob).days / 365.25)
    - `is_pediatric`  : age_years < 18

The age of the user is computed at the day-of-measurement (not at cohort
entry), so a user can cross from pediatric to adult mid-window. Pediatric
data is analyzed separately throughout PLN-1008.

DOBs come from the same source PLN-1001 uses (`bddp_user_dates` or
equivalent join key); confirm column name during Phase C implementation.

Output table columns: `_userId`, `day`, `age_years`, `is_pediatric`.

Status: Phase A stub — signature only. Implement in Phase C.
"""

from typing import Optional


def run(
    spark,
    user_dates_table: str = "dev.fda_510k_rwd.bddp_user_dates",
    output_table: str = "dev.fda_510k_rwd.nma_user_day_age",
    days_table: Optional[str] = None,
) -> None:
    """Compute per-day age and pediatric flag.

    Args:
        spark: SparkSession.
        user_dates_table: Table containing user DOB / first-record dates.
        output_table: Destination Unity Catalog table.
        days_table: Optional table enumerating the (user, day) grid;
                    defaults to deriving from the master day inventory.

    Side effects:
        Writes the output table.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
