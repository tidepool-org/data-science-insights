"""R01 ML for Diabetes power analysis: detect 5% absolute TIR improvement, 80% power, paired design."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from databricks.connect import DatabricksSession
from scipy.stats import norm

TABLE = "dev.fda_510k_rwd.loop_user_tir_unfiltered"
ALPHA = 0.05
POWER = 0.80
TARGET_EFFECT = 5.0
RHOS = [0.3, 0.5, 0.7]
POWERS = [0.70, 0.80, 0.90, 0.95]

OUT_DIR = Path.cwd()
FIG_DIR = OUT_DIR / "figures"


def paired_n(delta, sigma, alpha=ALPHA, beta=1 - POWER):
    """N to detect mean paired delta under normal approximation."""
    z = norm.ppf(1 - alpha / 2) + norm.ppf(1 - beta)
    return np.ceil((sigma * z / delta) ** 2)


def paired_power(n, delta, sigma, alpha=ALPHA):
    """Power for paired t-test at fixed N under normal approximation."""
    z_alpha = norm.ppf(1 - alpha / 2)
    ncp = delta / (sigma / np.sqrt(n))
    return 1 - norm.cdf(z_alpha - ncp) + norm.cdf(-z_alpha - ncp)


def sigma_delta(sigma_bs, rho):
    """Within-subject delta std implied by between-subject std and pre-post correlation."""
    return sigma_bs * np.sqrt(2 * (1 - rho))


def load_per_user_tir(table: str) -> pd.DataFrame:
    spark = DatabricksSession.builder.getOrCreate()
    return spark.read.table(table).toPandas()


def plot_n_vs_effect(sigma_bs: float, path: Path) -> None:
    deltas = np.linspace(1, 10, 200)
    sigmas = [0.5 * sigma_bs, sigma_bs, 1.5 * sigma_bs]
    fig, ax = plt.subplots(figsize=(7, 5))
    for sigma in sigmas:
        ax.plot(deltas, paired_n(deltas, sigma), label=f"σ = {sigma:.1f}%")
    ax.axvline(TARGET_EFFECT, color="gray", linestyle="--", alpha=0.6, label=f"Δ = {TARGET_EFFECT}%")
    ax.set_xlabel("Absolute TIR improvement (%)")
    ax.set_ylabel("Required N (paired)")
    ax.set_title(f"Sample size vs effect size  (power={POWER:.0%}, α={ALPHA})")
    ax.set_ylim(0, 200)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_n_vs_sigma(sigma_bs: float, path: Path) -> None:
    sigmas = np.linspace(sigma_bs * 0.5, sigma_bs * 2.0, 200)
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(sigmas, paired_n(TARGET_EFFECT, sigmas), label="ρ = 0.5 (σ_δ = σ_BS)")
    for rho in RHOS:
        ax.plot(sigmas, paired_n(TARGET_EFFECT, sigma_delta(sigmas, rho)),
                linestyle="--", label=f"ρ = {rho:.1f}")
    ax.axvline(sigma_bs, color="gray", linestyle="--", alpha=0.6, label=f"observed σ_BS = {sigma_bs:.1f}%")
    ax.set_xlabel("Standard deviation of TIR (%)")
    ax.set_ylabel("Required N (paired)")
    ax.set_title(f"Sample size vs variance  (Δ={TARGET_EFFECT}%, power={POWER:.0%}, α={ALPHA})")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_power_vs_n(sigma_bs: float, path: Path) -> None:
    ns = np.arange(10, 301)
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(ns, paired_power(ns, TARGET_EFFECT, sigma_bs), label="ρ = 0.5 (σ_δ = σ_BS)")
    for rho in RHOS:
        s_d = sigma_delta(sigma_bs, rho)
        ax.plot(ns, paired_power(ns, TARGET_EFFECT, s_d),
                linestyle="--", label=f"ρ = {rho:.1f} (σ_δ = {s_d:.1f}%)")
    ax.axhline(POWER, color="gray", linestyle="--", alpha=0.6, label=f"power = {POWER:.0%}")
    ax.set_xlabel("Sample size N")
    ax.set_ylabel("Power")
    ax.set_title(f"Power vs sample size  (Δ={TARGET_EFFECT}%, σ_BS={sigma_bs:.1f}%, α={ALPHA})")
    ax.set_ylim(0, 1.02)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_n_heatmap(sigma_bs: float, path: Path) -> None:
    deltas = np.linspace(1, 10, 50)
    sigmas = np.linspace(sigma_bs * 0.5, sigma_bs * 2.0, 50)
    D, S = np.meshgrid(deltas, sigmas)
    N = paired_n(D, S)

    fig, ax = plt.subplots(figsize=(8, 6))
    pcm = ax.pcolormesh(D, S, np.log10(N), shading="auto", cmap="viridis")
    cbar = fig.colorbar(pcm, ax=ax)
    cbar.set_label("log10(N required)")

    levels = [10, 20, 50, 100, 200, 500, 1000]
    cs = ax.contour(D, S, N, levels=levels, colors="white", linewidths=0.8)
    ax.clabel(cs, fmt="%d", fontsize=8)

    n_target = int(paired_n(TARGET_EFFECT, sigma_bs))
    ax.scatter([TARGET_EFFECT], [sigma_bs], color="red", s=60, zorder=5,
               label=f"observed (Δ={TARGET_EFFECT}%, σ={sigma_bs:.1f}%) → N={n_target}")
    ax.set_xlabel("Absolute TIR improvement (%)")
    ax.set_ylabel("Standard deviation of TIR (%)")
    ax.set_title(f"Required N — paired, ρ = 0.5 (σ_δ = σ_BS)  (power={POWER:.0%}, α={ALPHA})")
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_effect_vs_n_powers(sigma_bs: float, path: Path) -> None:
    ns = np.arange(10, 301)
    z_alpha = norm.ppf(1 - ALPHA / 2)
    fig, ax = plt.subplots(figsize=(7, 5))
    for power in POWERS:
        z = z_alpha + norm.ppf(power)
        min_delta = sigma_bs * z / np.sqrt(ns)
        ax.plot(ns, min_delta, label=f"power = {power:.0%}")
    ax.axhline(TARGET_EFFECT, color="gray", linestyle="--", alpha=0.6, label=f"Δ = {TARGET_EFFECT}%")
    ax.set_xlabel("Sample size N")
    ax.set_ylabel("Minimum detectable TIR improvement (%)")
    ax.set_title(f"Detectable effect vs sample size  (σ_BS = {sigma_bs:.1f}%, α = {ALPHA})")
    ax.set_ylim(0, 15)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_tir_distribution(df: pd.DataFrame, sigma_bs: float, mean_tir: float, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.hist(df["tir"], bins=40, edgecolor="white", alpha=0.85)
    ax.axvline(mean_tir, color="black", label=f"mean = {mean_tir:.1f}%")
    ax.axvline(mean_tir - sigma_bs, color="gray", linestyle="--", label=f"±1σ_BS = {sigma_bs:.1f}%")
    ax.axvline(mean_tir + sigma_bs, color="gray", linestyle="--")
    ax.set_xlabel("Per-user cleaned TIR (%)")
    ax.set_ylabel("Number of users")
    ax.set_title(f"Per-user TIR distribution  (n = {len(df)})")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main():
    FIG_DIR.mkdir(exist_ok=True)
    df = load_per_user_tir(TABLE)

    n_users = len(df)
    mean_tir = df["tir"].mean()
    sigma_bs = df["tir"].std()
    median_tir = df["tir"].median()
    q1, q3 = df["tir"].quantile([0.25, 0.75])
    mean_days = df["qualifying_days"].mean()

    n_required = int(paired_n(TARGET_EFFECT, sigma_bs))
    n_required_by_rho = {rho: int(paired_n(TARGET_EFFECT, sigma_delta(sigma_bs, rho))) for rho in RHOS}

    print(f"n_users                 = {n_users}")
    print(f"mean qualifying days    = {mean_days:.1f}")
    print(f"mean TIR                = {mean_tir:.2f}%")
    print(f"median TIR (IQR)        = {median_tir:.2f}% ({q1:.2f}–{q3:.2f})")
    print(f"σ_BS TIR                = {sigma_bs:.2f}%")
    print()
    print(f"Required N for Δ={TARGET_EFFECT}%, paired, α={ALPHA}, power={POWER:.0%}:")
    print(f"  ρ = 0.5 (σ_δ = σ_BS): N = {n_required}")
    for rho, n in n_required_by_rho.items():
        print(f"  ρ = {rho:.1f} (σ_δ = {sigma_delta(sigma_bs, rho):.2f}%): N = {n}")

    plot_n_vs_effect(sigma_bs, FIG_DIR / "fig1_n_vs_effect.png")
    plot_n_vs_sigma(sigma_bs, FIG_DIR / "fig2_n_vs_sigma.png")
    plot_power_vs_n(sigma_bs, FIG_DIR / "fig3_power_vs_n.png")
    plot_n_heatmap(sigma_bs, FIG_DIR / "fig4_n_heatmap.png")
    plot_tir_distribution(df, sigma_bs, mean_tir, FIG_DIR / "fig5_tir_distribution.png")
    plot_effect_vs_n_powers(sigma_bs, FIG_DIR / "fig6_effect_vs_n_powers.png")

    summary = pd.DataFrame([{
        "n_users": n_users,
        "mean_qualifying_days": mean_days,
        "mean_tir": mean_tir,
        "median_tir": median_tir,
        "tir_q1": q1,
        "tir_q3": q3,
        "std_tir_bs": sigma_bs,
        "target_effect_pct": TARGET_EFFECT,
        "alpha": ALPHA,
        "power": POWER,
        **{f"n_required_paired_rho_{rho:.1f}": n for rho, n in n_required_by_rho.items()},
    }])
    summary.to_csv(OUT_DIR / "summary.csv", index=False)
    print(f"\nWrote {OUT_DIR / 'summary.csv'} and {FIG_DIR}/fig*.png")


if __name__ == "__main__":
    main()
