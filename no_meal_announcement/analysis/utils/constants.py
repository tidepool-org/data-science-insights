"""
Shared constants for the PLN-1008 NMA analysis.

Re-exports figure styling (FONT, COLORS_*) from
`FDA_real_world_data.analysis.utils.constants` so plot styling stays consistent
across the regulatory submission. PLN-1008-specific constants are defined below.
"""

import os
import sys

try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _here = "/Workspace/Users/mark.connolly@tidepool.org/data-science-insights/no_meal_announcement/analysis/utils"
_fda_utils = os.path.normpath(
    os.path.join(_here, "..", "..", "..", "FDA_real_world_data", "analysis", "utils")
)
if _fda_utils not in sys.path:
    sys.path.insert(0, _fda_utils)

from constants import (  # type: ignore # noqa: E402
    FONT,
    COLORS_PRIMARY,
    COLORS_SECONDARY,
    COLORS_ACCENT,
    COLORS_STACKED_BAR,
)

__all__ = [
    "FONT",
    "COLORS_PRIMARY",
    "COLORS_SECONDARY",
    "COLORS_ACCENT",
    "COLORS_STACKED_BAR",
    "R_THRESHOLD",
    "MIN_USER_DAYS",
    "MIN_COVERAGE_PCT",
    "MIN_AUTOBOLUS_COUNT",
    "MIN_DAYS_FOR_TDD_PAIR",
    "BOOTSTRAP_SEED",
    "BOOTSTRAP_RESAMPLES",
    "DAY_TYPE_ORDER",
    "DELIVERY_STRATEGY_ORDER",
    "GLYCEMIC_ENDPOINTS",
    "PRIMARY_ENDPOINT",
    "GLYCEMIC_RANGES_MGDL",
]

R_THRESHOLD: float = 1.0
"""Low/High TDD stratification cutpoint (PLN-1008 §7.5)."""

MIN_USER_DAYS: int = 10
"""User inclusion threshold (PLN-1008 §7.1)."""

MIN_COVERAGE_PCT: float = 0.70
"""Day-inclusion CGM coverage threshold (PLN-1008 §7.1)."""

MIN_AUTOBOLUS_COUNT: int = 3
"""Autobolus-detection threshold per day (PLN-1008 §7.3)."""

MIN_DAYS_FOR_TDD_PAIR: int = 30
"""User threshold for Analysis 3 paired-contrast eligibility (PLN-1008 §8.3)."""

BOOTSTRAP_SEED: int = 20260520
"""Deterministic seed for cluster-bootstrap CIs (audit-friendly)."""

BOOTSTRAP_RESAMPLES: int = 1000
"""Number of bootstrap resamples (PLN-1008 §8.1)."""

DAY_TYPE_ORDER: tuple = (
    "CE=0/BE=0",
    "CE=0/BE≤1",
    "CE=0/BE≤∞",
    "CE>0",
)
"""Canonical ordering for plot legends and table columns."""

DELIVERY_STRATEGY_ORDER: tuple = ("autobolus", "temp_basal")
"""Canonical ordering for Analysis 2 marginal-cell tables."""

GLYCEMIC_ENDPOINTS: tuple = (
    "pct_time_lt_54",
    "pct_time_lt_70",
    "pct_time_70_180",
    "pct_time_gt_180",
    "pct_time_gt_250",
    "mean_glucose_mgdl",
    "cv_pct",
    "hypo_events",
)
"""Endpoint column names produced by `compute_nma_glycemic_endpoints`."""

PRIMARY_ENDPOINT: str = "pct_time_70_180"
"""TIR 70-180 mg/dL is the pre-specified primary endpoint (PLN-1008 §8.4)."""

GLYCEMIC_RANGES_MGDL: tuple = (
    (None, 54),
    (54, 70),
    (70, 180),
    (180, 250),
    (250, None),
)
"""Stacked-bar bins for Figure 8.1a / 8.2d / 8.3c."""
