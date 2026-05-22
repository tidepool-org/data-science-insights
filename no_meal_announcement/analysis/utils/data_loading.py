"""
Master-table loader and cohort filters for PLN-1008 analyses.

Reads `dev.fda_510k_rwd.nma_user_day_master`, applies the cohort selection
(adult / pediatric / all), and applies the CE>0 comparator restriction
required by Tables 8.1a–b: "CE>0 arm restricted to users who contributed
≥1 CE=0 day in at least one of the three NMA-like classifications."

Reuses `COHORT_WHERE`, `MAX_LOOP_VERSION_INT`, `MIN_AGE` from
`FDA_real_world_data.analysis.utils.data_loading` so the PLN-1008 cohort
matches PLN-1001 verbatim.

Status: Phase A stub — signatures only. Implement in Phase C.
"""

from typing import Literal

import pandas as pd


def load_user_day_master(
    spark,
    cohort: Literal["adult", "pediatric", "all"] = "adult",
    table: str = "dev.fda_510k_rwd.nma_user_day_master",
) -> pd.DataFrame:
    """Load the analysis-ready user-day master table.

    Args:
        spark: SparkSession.
        cohort: 'adult' (age ≥18), 'pediatric' (age <18), or 'all'.
        table: Master table name.

    Returns:
        pandas DataFrame with one row per surviving user-day.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def restrict_ce_pos_comparator(df: pd.DataFrame) -> pd.DataFrame:
    """Restrict the CE>0 comparator arm.

    Per Table 8.1a footnote: CE>0 days are kept only for users who
    contributed ≥1 CE=0 day in at least one of the three NMA-like
    classifications.

    Applied to Analyses 1 and 2; not applicable to Analysis 3 (CE=0 only).
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
