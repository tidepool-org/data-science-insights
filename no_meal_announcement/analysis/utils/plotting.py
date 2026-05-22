"""
Reusable plot recipes for PLN-1008 figures.

Recipe set:
    plot_paired_violin_box      — Figures 8.1b, 8.1c, 8.2a, 8.2b, 8.3a
    plot_stacked_bar_tir        — Figures 8.1a, 8.2d, 8.3c
    plot_paired_diff_hist       — Figure 8.3b
    plot_interaction_lines      — Figure 8.2c
    plot_r_distribution         — Figure 8.3d

Each recipe takes a long-format DataFrame and an output path, saves a PNG,
and returns nothing. Styling (font sizes, color schemes) is sourced from
`FDA_real_world_data.analysis.utils.constants`.

Status: Phase A stub — signatures only. Implement in Phase C.
"""

from pathlib import Path

import pandas as pd


def plot_paired_violin_box(
    df: pd.DataFrame,
    arm_col: str,
    endpoint_col: str,
    output_path: Path,
    *,
    title: str = "",
    arm_order: tuple = (),
    overlay_paired_lines: bool = False,
) -> None:
    """Per-user violin + box + jittered points by arm.

    Figures 8.1b/c, 8.2a/b, 8.3a. Per-user values are required (compute
    them upstream by averaging within arm per user).
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def plot_stacked_bar_tir(
    df: pd.DataFrame,
    group_col: str,
    output_path: Path,
    *,
    title: str = "",
    group_order: tuple = (),
    annotate_counts: bool = True,
) -> None:
    """Stacked bar of mean percent time in glycemic ranges per group.

    Figures 8.1a, 8.2d, 8.3c. Per-bar user-day and user counts are annotated
    when `annotate_counts` is True.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def plot_paired_diff_hist(
    diffs: pd.DataFrame,
    endpoint_cols: tuple,
    output_path: Path,
    *,
    title: str = "",
    zero_line: bool = True,
) -> None:
    """Histogram panel of per-user paired differences.

    Figure 8.3b: distribution of within-user (Low-TDD − High-TDD) differences
    across endpoints, one subplot per endpoint, with a zero reference line.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def plot_interaction_lines(
    marginal_means: pd.DataFrame,
    output_path: Path,
    *,
    title: str = "",
    x_col: str = "day_type",
    series_col: str = "delivery_strategy",
    y_col: str = "mean",
) -> None:
    """Interaction plot of model-estimated marginal means.

    Figure 8.2c: lines + points showing day_type effect by delivery_strategy;
    non-parallel lines indicate interaction visually.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")


def plot_r_distribution(
    per_day: pd.DataFrame,
    output_path: Path,
    *,
    title: str = "",
    r_col: str = "R_user_day",
    classification_col: str = "day_type_strictest",
    threshold: float = 1.0,
) -> None:
    """Violin + box of R_user_day across CE=0 days, with R=1.0 cut line.

    Figure 8.3d: one panel per day classification.
    """
    raise NotImplementedError("Phase A stub — implement in Phase C")
