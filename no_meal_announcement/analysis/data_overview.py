"""
Cohort and day-count overview for PLN-1008.

Descriptive characterizations (not part of the inferential analyses):
    - CONSORT-style flow chart: BDDP records → cohort filter → day inclusion
      → day-type classification → analysis-ready counts.
    - Distribution of PAF settings across the surviving cohort (descriptive
      since PAF=0.4 is enforced, but reports any leakage).
    - Frequency and per-user count of each NMA-like day type
      (proportion of users contributing any NMA day; distribution of
      NMA-day counts per user, per classification — PLN-1008 §4 Secondary
      Objective bullet 3).
    - Behavioral summary of bolus days (mean and median meal boluses per
      day; mean and median manual/correction boluses per day — Table 8.1c
      precursor).

Status: Phase A stub — signatures only. Implement in Phase C.
"""

from pathlib import Path

import pandas as pd


def consort_flow(spark, output_path: Path) -> None:
    """CONSORT-style flow chart of record/day/user reductions."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def paf_distribution(df: pd.DataFrame, output_path: Path) -> None:
    """Bar chart of PAF setting frequency across the surviving cohort."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def nma_day_frequency(df: pd.DataFrame, output_path: Path) -> None:
    """Per-user counts of each NMA-like day type and CE>0 days."""
    raise NotImplementedError("Phase A stub — implement in Phase C")


def main(spark, output_root: Path = Path("analysis/outputs")) -> None:
    """Run all data-overview reports for adult + pediatric cohorts."""
    raise NotImplementedError("Phase A stub — implement in Phase C")
