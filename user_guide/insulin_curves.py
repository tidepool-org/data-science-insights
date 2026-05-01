"""Generate insulin absorption curves for the Tidepool User Guide.

Uses the exponential insulin model (Loop / PyLoopKit) parameterized by
duration (td) and peak activity time (tp), both in minutes.
    Rapid-acting adult (Humalog/Novolog): td=360, tp=75
    Ultra-rapid acting (Fiasp/Lyumjev):   td=360, tp=55
"""

from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt


OUTPUT_DIR = Path(__file__).parent
DOSE_UNITS = 2.0
DELAY_MIN = 5.0


def insulin_activity(t_min, td, tp):
    tau = tp * (1 - tp / td) / (1 - 2 * tp / td)
    a = 2 * tau / td
    S = 1 / (1 - a + (1 + a) * np.exp(-td / tau))
    activity = (S / tau**2) * t_min * (1 - t_min / td) * np.exp(-t_min / tau)
    activity = np.where((t_min >= 0) & (t_min <= td), activity, 0.0)
    return activity


def plot_absorption_curves(curves, dose, delay_min, title, out_path):
    fig, ax = plt.subplots(figsize=(10, 4.5))

    td_max = 0
    for curve in curves:
        td = curve["td"]
        tp = curve["tp"]
        t_min = np.linspace(0, td + delay_min, 1000)
        t_post_delay = np.maximum(t_min - delay_min, 0)
        active = t_min >= delay_min
        absorption = insulin_activity(t_post_delay, td=td, tp=tp) * 60 * dose
        absorption = np.where(active, absorption, 0.0)
        ax.plot(
            t_min / 60, absorption,
            color=curve["color"], linewidth=3, label=curve["label"],
        )
        td_max = max(td_max, td + delay_min)

    ax.set_title(title, loc="left", color="#1a2a5e", fontsize=17, fontweight="bold")
    ax.set_xlabel("Time Since Delivery (Hours)", color="#3a6df0", fontsize=14)
    ax.set_xlim(0, 6.25)
    ax.set_ylim(0, None)
    ax.set_xticks(np.arange(0, 7, 1))
    ax.grid(True, color="#bcd0ff", linewidth=0.8)
    for spine in ax.spines.values():
        spine.set_color("#bcd0ff")
    ax.tick_params(colors="#3a6df0", labelsize=12)
    legend = ax.legend(loc="upper right", fontsize=13, facecolor="white", edgecolor="white")
    legend.get_frame().set_alpha(1.0)

    fig.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    plot_absorption_curves(
        curves=[
            {"td": 360, "tp": 75, "color": "#3a6df0", "label": "Rapid-Acting (peak 75 min)"},
            {"td": 360, "tp": 55, "color": "#f08a3a", "label": "Ultra-Rapid Acting (peak 55 min)"},
        ],
        dose=DOSE_UNITS,
        delay_min=DELAY_MIN,
        title="Insulin Absorption (U/hr)",
        out_path=OUTPUT_DIR / "insulin_absorption_comparison.png",
    )
    print(f"Wrote curve to {OUTPUT_DIR}")
