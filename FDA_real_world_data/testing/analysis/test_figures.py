"""Offline tests of analysis/utils/figures.py: the band derivation (hand-computed), the
equal-user mean, and the shared stacked-bar drawer on a headless axis."""

import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..", "..", "analysis")))
sys.path.insert(0, os.path.normpath(os.path.join(_here, "..")))
from offline_test_helpers import run_tests  # noqa: E402

from utils.figures import (  # noqa: E402
    BANDS,
    connect_band_boundaries,
    disjoint_bands,
    mean_bands_by_user,
    stacked_bar_with_callouts,
)


def _days():
    """Three user-days: two of user a, one of user b. Cumulative endpoints in percent."""
    return pd.DataFrame({
        "_userId":      ["a", "a", "b"],
        "tbr_very_low": [1.0, 3.0, 0.0],
        "tbr":          [4.0, 6.0, 2.0],
        "tir":          [70.0, 60.0, 80.0],
        "tar":          [26.0, 34.0, 18.0],
        "tar_very_high": [6.0, 10.0, 3.0],
    })


def test_disjoint_bands_cut_the_cumulative_endpoints():
    bands = disjoint_bands(_days())
    assert list(bands.columns) == BANDS
    assert bands.iloc[0].tolist() == [1.0, 3.0, 70.0, 20.0, 6.0]      # 4-1, 26-6
    assert bands.iloc[2].tolist() == [0.0, 2.0, 80.0, 15.0, 3.0]
    assert (bands.sum(axis=1) == 100.0).all(), "the five bands partition the day"


def test_mean_bands_by_user_weights_users_equally():
    means, n_users = mean_bands_by_user(_days())
    assert n_users == 2
    # user a's mean in-range time is 65, user b's is 80: the equal-user mean is 72.5, not the
    # day-weighted 70
    assert means["70-180"] == 72.5
    assert means["<54"] == 1.0 and means[">250"] == 5.5
    empty_means, empty_n = mean_bands_by_user(_days().iloc[0:0])
    assert empty_n == 0 and all(v == 0.0 for v in empty_means.values())


def test_stacked_bar_drawer_returns_the_band_tops():
    fig, ax = plt.subplots()
    means, _ = mean_bands_by_user(_days())
    tops = stacked_bar_with_callouts(ax, 0, means, 0.8, legend=True)
    assert len(tops) == 5 and abs(tops[-1] - 100.0) < 1e-9, tops
    assert len(ax.patches) == 5, "one bar per band"
    tops2 = stacked_bar_with_callouts(ax, 1, means, 0.8, legend=False)
    connect_band_boundaries(ax, 0, 1, tops, tops2, 0.8)
    assert len(ax.lines) == 4, "a connector per boundary, the top of the stack left out"
    plt.close(fig)


if __name__ == "__main__":
    run_tests([
        test_disjoint_bands_cut_the_cumulative_endpoints,
        test_mean_bands_by_user_weights_users_equally,
        test_stacked_bar_drawer_returns_the_band_tops,
    ], "test_figures")
