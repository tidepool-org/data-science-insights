"""Shared figure pieces for the FDA analyses: the stacked glycemic-range bar.

One drawing of the five-band stacked bar for every analysis that shows
time in ranges (MC, 2026-11-05: "Is there a unified stacked bar plotter
across the whole project?" There was not; analysis 8-1, IR-2, IR-6, 8-7,
the RFAI outcomes, and the NMA figures each drew their own). The
conventions are the IR-2 figure's, the house pattern since 2026-08:

- the five bands in COLORS_STACKED_BAR, bottom to top <54, 54-70, 70-180,
  180-250, >250, white seams between them;
- an in-bar label on a band tall enough to hold one (IN_BAR_LABEL_MIN);
- the <54 and 54-70 bands are too thin for in-bar labels, so each bar gets
  two unboxed callouts with a fixed-length leader from the top of its band,
  the <70 total on the left and <54 on the right;
- where two bars are compared (temporary basal beside autobolus), dashed
  connectors join each band boundary of one bar to the same boundary of
  the other, the top of the stack left out.

Analysis 8-1 draws through these; the other analyses move to them when
their reports are next refreshed (execution plan Step 7).
"""
from typing import Dict, List

from utils.constants import COLORS_STACKED_BAR, FONT, STACKED_BAR_PCT_DECIMALS

BANDS = ["<54", "54-70", "70-180", "180-250", ">250"]
BAND_LABELS = ["<54 mg/dL\n(Very Low)", "54-70 mg/dL\n(Low)", "70-180 mg/dL\n(In Range)",
               "180-250 mg/dL\n(High)", ">250 mg/dL\n(Very High)"]
LOW_BANDS = ("<54", "54-70")
LOW_BAND_LEADER = 6.0       # leader length in percent-of-time units
LOW_BAND_CALLOUT_DX = 0.16  # the callouts' offsets from the bar's centre, at a bar width of 0.5
IN_BAR_LABEL_MIN = 4.0      # a band shorter than this gets no in-bar label
CONNECTOR_TOP_LIMIT = 99.0  # a boundary at or above this is the top of the stack


def percent(value: float) -> str:
    return f"{value:.{STACKED_BAR_PCT_DECIMALS}f}%"


def stacked_bar_with_callouts(ax, x: float, means: Dict[str, float], width: float, legend: bool) -> List[float]:
    """One stacked bar of the five bands at x, in-bar labels on the bands
    tall enough to hold one, and the two low-band callouts. `means` maps
    each band to its percent of time. Returns the band boundaries
    (cumulative tops) for connect_band_boundaries."""
    bottom, tops = 0.0, []
    for band, band_label in zip(BANDS, BAND_LABELS):
        v = max(0.0, means[band])
        ax.bar(x, v, width, bottom=bottom, color=COLORS_STACKED_BAR[band],
               label=band_label if legend else "_nolegend_", edgecolor="white", lw=0.5)
        if v >= IN_BAR_LABEL_MIN and band not in LOW_BANDS:
            ax.text(x, bottom + v / 2, percent(v), ha="center", va="center", fontsize=FONT["annotation"])
        bottom += v
        tops.append(bottom)
    below_54 = max(0.0, means["<54"])
    below_70 = below_54 + max(0.0, means["54-70"])
    dx = LOW_BAND_CALLOUT_DX * width / 0.5
    for offset, anchor, text in ((-dx, below_70, percent(below_70)), (dx, below_54, percent(below_54))):
        ax.annotate(text, xy=(x + offset, anchor), xytext=(x + offset, anchor + LOW_BAND_LEADER),
                    ha="center", va="bottom", fontsize=FONT["annotation"], color="black",
                    arrowprops={"arrowstyle": "-", "color": "#555555", "linewidth": 1.0})
    return tops


def connect_band_boundaries(ax, x1: float, x2: float, tops1: List[float], tops2: List[float], width: float):
    """Dashed connectors from each band boundary of the bar at x1 to the same
    boundary of the bar at x2, the top of the stack left out."""
    for top1, top2 in zip(tops1, tops2):
        if top1 < CONNECTOR_TOP_LIMIT and top2 < CONNECTOR_TOP_LIMIT:
            ax.plot([x1 + width / 2, x2 - width / 2], [top1, top2], ls="--", color="black", lw=1, alpha=1)
