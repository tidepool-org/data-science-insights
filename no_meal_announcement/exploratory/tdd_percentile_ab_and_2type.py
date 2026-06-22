"""Exploratory variants of the §8.3 "TIR vs within-user TDD percentile" scatter (fig 8.3e / 12.3h).

Renders three views requested 2026-06-15, reusing the production figure builder
`figure_8_3e_tir_vs_tdd_pct` (parameterized with `day_types` / `delivery_strategy`) so there is one
definition of the scatter — no fork:

  1. ab_5type     — the full 5-day-type scatter restricted to AB (autobolus_on) days.
  2. 2type        — all eligible days, only the CE=0/BE=0 vs CE>=3/BE>=3 contrast (no-announcement
                    vs high-announcement extremes).
  3. 2type_ab     — (2) restricted to AB days.

Like the production figure, NONE of these is same-user-set gated: the scatter plots EVERY eligible
day at its within-user TDD percentile (the continuous relationship), so a user need not have a day in
each tercile — the gate is a property of the rank-tercile TABLES (8.3d/12.3g/12.3h-table) only. The
x-axis is the within-user TDD percentile over ALL eligible days (the "overall" reference); the AB/2-type
restriction only changes which days are *shown*, not the percentile basis, so the axis stays comparable
to fig 8.3e. The shaded bands are the overall-ref tercile cuts (RANK_TERCILES = 1/3, 2/3).

Prep mirrors the production run() exactly (prepare_day_level -> filter_cohort -> restrict_comparator)
so the underlying cohort is identical to fig 8.3e — these are sub-selections of it.

Run (cohort=all by default; matches the n=580,136 reference):
  /Users/mconn/miniconda3/envs/tidepool-data-science-simulator-dev/bin/python \
      no_meal_announcement/exploratory/tdd_percentile_ab_and_2type.py [--cohort all|adult|pediatric]
"""
import argparse
import importlib.util
import os
import sys

import matplotlib
matplotlib.use("Agg")
import pandas as pd  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.normpath(os.path.join(HERE, "..", "analysis"))
OUT_DIR = os.path.normpath(os.path.join(HERE, "..", "outputs", "tdd_pct_ab_2type"))
CSV = os.path.normpath(os.path.join(HERE, "..", "outputs", "nma_user_day_analysis_ready.csv"))
sys.path.insert(0, ANALYSIS_DIR)

from utils.data_loader import (  # noqa: E402
    CLASSIFICATIONS,
    HIGH_MA_LABEL,
    MIN_AGE,
    filter_cohort,
    prepare_day_level,
    restrict_comparator,
)

AB = "autobolus_on"
TWO_TYPE = (CLASSIFICATIONS[0][1], HIGH_MA_LABEL)   # ("CE=0/BE=0", "CE>=3/BE>=3")


def _load_fig_builder():
    """Import figure_8_3e_tir_vs_tdd_pct from the hyphenated analysis_8-3 module (which can't be a
    normal `import`) by path, reusing the established spec_from_file_location pattern."""
    path = os.path.join(ANALYSIS_DIR, "analysis_8-3_nma_tdd_stratified.py")
    spec = importlib.util.spec_from_file_location("analysis_8_3", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.figure_8_3e_tir_vs_tdd_pct


def main(cohort="all"):
    os.makedirs(OUT_DIR, exist_ok=True)
    if not os.path.exists(CSV):
        raise FileNotFoundError(
            f"analysis-ready CSV not found at {CSV}. Run "
            "data_staging/export_user_day_analysis_ready.py first.")
    figure = _load_fig_builder()

    pdf = prepare_day_level(pd.read_csv(CSV))
    pdf = filter_cohort(pdf, cohort=cohort, min_age=MIN_AGE)
    pdf = restrict_comparator(pdf)

    common = dict(tercile_bands=False, show_age_breakdown=True)
    specs = [
        ("figure_8_3e_ab_tir_vs_tdd_percentile.png",
         dict(delivery_strategy=AB, **common)),
        ("figure_8_3e_2type_tir_vs_tdd_percentile.png",
         dict(day_types=TWO_TYPE, **common)),
        ("figure_8_3e_2type_ab_tir_vs_tdd_percentile.png",
         dict(day_types=TWO_TYPE, delivery_strategy=AB, **common)),
    ]
    for fname, kw in specs:
        fig = figure(pdf, **kw)
        out = os.path.join(OUT_DIR, f"{cohort}__{fname}")
        fig.savefig(out, dpi=150, bbox_inches="tight")  # expand canvas to fit the outside legend
        print(f"wrote -> {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", default="all", choices=["all", "adult", "pediatric"])
    args, _ = ap.parse_known_args()
    main(cohort=args.cohort)
