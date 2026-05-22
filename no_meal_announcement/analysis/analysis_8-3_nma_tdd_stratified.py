"""
Analysis 8.3: Glycemic Outcomes on NMA-like Days, Stratified by Within-User TDD.

PLN-1008 §8.3. On CE=0 days (for each of the three nested classifications),
within each user compute the mean endpoint on Low-TDD days vs High-TDD days
(strata defined by R_user_day = TDD_day / mean_TDD_user, cut at 1.0).

Primary test: Wilcoxon signed-rank on the per-user (Low − High) differences.
Parametric companion: paired t.
Day-level sensitivity: LMM `outcome ~ tdd_stratum + (1 | user)`.

Sensitivity analyses:
    - Tercile cutpoints (bottom vs top tercile of each user's CE=0-day R distribution).
    - Median TDD reference (replaces mean reference).
    - Rolling 30-day TDD reference (per §7.5).

Outputs:
    Table 8.3a — per-user summary by TDD stratum.
    Table 8.3b — within-user paired contrast estimates.
    Table 8.3c — LMM day-level sensitivity.
    Figure 8.3a — paired-difference visualization per endpoint (one row per classification).
    Figure 8.3b — histograms of paired differences.
    Figure 8.3c — stacked bar comparison on CE=0 days by stratum (with CE>0 reference bar).
    Figure 8.3d — within-user R_user_day distribution.

Eligibility: ≥30 eligible days for personal TDD reference AND ≥1 CE=0 day
per stratum (per classification under analysis).

Status: Phase A stub — signatures only. Implement in Phase C.
"""

from pathlib import Path
from typing import Literal

import pandas as pd


def load_data(spark, cohort: Literal["adult", "pediatric", "all"]) -> pd.DataFrame:
    """Load master and restrict to CE=0 days; preserve the three nested
    classifications as separate boolean columns."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def create_table_8_3a(df: pd.DataFrame) -> pd.DataFrame:
    """Per-user summary (mean ± SD, median [IQR]) by Low/High TDD stratum,
    repeated for each of the three day classifications."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def create_table_8_3b(df: pd.DataFrame) -> pd.DataFrame:
    """Within-user paired Low−High contrast: median + mean differences with
    bootstrap 95% CI, Wilcoxon p, paired-t p, N pairs."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def create_table_8_3c(df: pd.DataFrame) -> pd.DataFrame:
    """LMM day-level sensitivity: outcome ~ tdd_stratum + (1 | user)."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def figure_8_3a(df: pd.DataFrame, output_dir: Path) -> None:
    """Paired-difference visualization per endpoint, one row per classification."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def figure_8_3b(df: pd.DataFrame, output_dir: Path) -> None:
    """Histograms of within-user (Low − High) differences."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def figure_8_3c(df: pd.DataFrame, output_dir: Path) -> None:
    """Stacked bar of glycemic ranges by stratum, with CE>0 reference."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def figure_8_3d(df: pd.DataFrame, output_dir: Path) -> None:
    """Within-user R_user_day distribution; R=1.0 threshold marked."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def sensitivity_terciles(df: pd.DataFrame) -> pd.DataFrame:
    """Rerun Table 8.3b using bottom vs top tercile of each user's CE=0-day R."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def sensitivity_median_reference(df: pd.DataFrame) -> pd.DataFrame:
    """Rerun Table 8.3b using the median TDD reference instead of mean."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def sensitivity_rolling_30d(df: pd.DataFrame) -> pd.DataFrame:
    """Rerun Table 8.3b using the rolling-30-day TDD reference."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def main(spark, output_root: Path = Path("analysis/outputs")) -> None:
    """Orchestrate adult + pediatric runs of Analysis 8.3 plus sensitivities."""
    raise NotImplementedError("Phase A stub — implement in Phase C")
