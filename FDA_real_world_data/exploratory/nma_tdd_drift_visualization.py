"""
Exploratory: per-user TDD timeline visualization.

Surfaces users whose insulin needs drift materially during the PLN-1001
analysis window, providing visual context for why a rolling-30-day TDD
reference is necessary as a sensitivity analysis (PLN-1008 §7.5).

Produces:
    - Per-user line plots of daily `tdd_u` over the window.
    - Highlights `mean_tdd_user`, `median_tdd_user`, and the trailing
      `rolling_30d_tdd` curve.
    - Flags users with high coefficient of variation in TDD over the window.

Status: Phase A stub. Implement after master table is built.
"""

from pathlib import Path


def main(spark, output_dir: Path = Path("analysis/outputs/exploratory")) -> None:
    """Plot per-user TDD timelines."""
    raise NotImplementedError("Phase C exploratory — implement after master table lands")
