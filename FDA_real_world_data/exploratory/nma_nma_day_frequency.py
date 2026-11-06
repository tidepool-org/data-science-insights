"""
Exploratory: frequency and per-user distribution of NMA-like days.

Not part of the inferential analysis. Descriptive characterization of the
cohort to support PLN-1008 §4 Secondary Objective bullet 3 ("Describe the
frequency and distribution of each no meal announcement day type").

Produces:
    - Proportion of users contributing ≥1 day in each NMA classification.
    - Distribution of NMA-day counts per user, per classification
      (histograms + summary stats).
    - Per-user fraction of days in each NMA-like arm.

Status: Phase A stub. Implement after master table is built.
"""

from pathlib import Path


def main(spark, output_dir: Path = Path("analysis/outputs/exploratory")) -> None:
    """Compute frequency tables and save histograms."""
    raise NotImplementedError("Phase C exploratory — implement after master table lands")
