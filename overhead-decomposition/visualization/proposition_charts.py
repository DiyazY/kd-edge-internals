#!/usr/bin/env python3
"""
Publication-quality figures for trade-off hypothesis validation results.

Generates:
1. Security vs resource cost scatter (H1/H6: security–resource trade-off)
2. Pod startup latency comparison — lightweight vs heavy (H3: lightweight advantage)
3. Efficiency bar chart (H4: efficiency per unit overhead)
4. Security vs maintainability scatter (H5: security–setup burden)
5. Hypothesis validation summary heatmap
"""

import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import FIGURES_DIR, KD_COLORS, KD_ORDER, RESULTS_DIR, SECURITY_SCORES
from visualization.style import FIGURE_SIZE, FIGURE_SIZE_WIDE, save_figure, setup_style

# Import proposition constants
from analysis.proposition_testing import (
    DP_THROUGHPUT_OPS,
    SETUP_HOURS,
    SYSTEM_IDLE_CPU_PCT,
    SYSTEM_IDLE_RAM_MIB,
)


def plot_security_vs_resources():
    """
    H1/H6: Scatter plot — CIS security score vs idle CPU and RAM.
    Two-panel figure showing the security–resource cost trade-off.
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=FIGURE_SIZE_WIDE)

    kds = KD_ORDER
    for kd in kds:
        sec = SECURITY_SCORES[kd]
        cpu = SYSTEM_IDLE_CPU_PCT[kd]
        ram = SYSTEM_IDLE_RAM_MIB[kd]
        color = KD_COLORS[kd]

        ax1.scatter(sec, cpu, c=color, s=100, zorder=5, edgecolors="black", linewidths=0.5)
        ax1.annotate(kd, (sec, cpu), textcoords="offset points",
                     xytext=(8, 5), fontsize=9)

        ax2.scatter(sec, ram, c=color, s=100, zorder=5, edgecolors="black", linewidths=0.5)
        ax2.annotate(kd, (sec, ram), textcoords="offset points",
                     xytext=(8, 5), fontsize=9)

    # Trend lines
    secs = [SECURITY_SCORES[kd] for kd in kds]
    cpus = [SYSTEM_IDLE_CPU_PCT[kd] for kd in kds]
    rams = [SYSTEM_IDLE_RAM_MIB[kd] for kd in kds]

    z_cpu = np.polyfit(secs, cpus, 1)
    p_cpu = np.poly1d(z_cpu)
    x_fit = np.linspace(min(secs) - 5, max(secs) + 5, 100)
    ax1.plot(x_fit, p_cpu(x_fit), "--", color="gray", alpha=0.5, linewidth=1)

    z_ram = np.polyfit(secs, rams, 1)
    p_ram = np.poly1d(z_ram)
    ax2.plot(x_fit, p_ram(x_fit), "--", color="gray", alpha=0.5, linewidth=1)

    ax1.set_xlabel("CIS Security Score (%)")
    ax1.set_ylabel("Idle CPU Usage (%)")
    ax1.set_title("Security vs CPU Overhead")

    ax2.set_xlabel("CIS Security Score (%)")
    ax2.set_ylabel("Idle RAM (MiB)")
    ax2.set_title("Security vs Memory Footprint")

    fig.suptitle("Security–Resource Cost Trade-off (ρ=0.89, p=0.04)", fontsize=13)
    fig.tight_layout()
    return save_figure(fig, "proposition_p1_p14_security_resources", FIGURES_DIR)


def plot_startup_latency_comparison():
    """
    H3: Grouped bar chart — pod startup latency per KD across test types.
    Shows lightweight (k3s, k0s) consistently faster than heavy group.
    """
    from analysis.proposition_testing import load_pod_startup_latency

    try:
        startup = load_pod_startup_latency()
    except FileNotFoundError:
        print("  Missing pod-startup-latency data")
        return

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    test_types = startup["tests"].unique()
    kds = KD_ORDER
    x = np.arange(len(test_types))
    width = 0.15
    offsets = np.arange(len(kds)) - (len(kds) - 1) / 2

    for i, kd in enumerate(kds):
        means = []
        stds = []
        for tt in test_types:
            tt_data = startup[(startup["kd"] == kd) & (startup["tests"] == tt)]
            means.append(tt_data["medians"].mean())
            stds.append(tt_data["medians"].std())

        is_lightweight = kd in ("k3s", "k0s")
        hatch = "" if is_lightweight else "///"
        ax.bar(x + offsets[i] * width, means, width, yerr=stds,
               label=kd, color=KD_COLORS[kd], edgecolor="white",
               capsize=2, hatch=hatch, alpha=0.85)

    test_labels = {
        "cp_light_1client": "CP Light\n(1 client)",
        "cp_heavy_8client": "CP Heavy\n(8 clients)",
        "cp_heavy_12client": "CP Heavy\n(12 clients)",
    }
    ax.set_xticks(x)
    ax.set_xticklabels([test_labels.get(t, t) for t in test_types])
    ax.set_ylabel("Median Pod Startup Latency (ms)")
    ax.set_title("Lightweight Distributions Have Lower Startup Latency")
    ax.legend(ncol=3, fontsize=8, loc="upper left")

    # Add annotation for lightweight group
    ax.annotate("Lightweight\n(solid bars)", xy=(0.02, 0.88),
                xycoords="axes fraction", fontsize=8,
                fontstyle="italic", color="gray")

    fig.tight_layout()
    return save_figure(fig, "proposition_p3_startup_latency", FIGURES_DIR)


def plot_efficiency_comparison():
    """
    H4: Bar chart — data-plane efficiency (throughput per unit CPU overhead).
    Shows k3s achieves 16× the efficiency of KubeEdge.
    """
    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    kds = KD_ORDER
    efficiency = {
        kd: DP_THROUGHPUT_OPS[kd] / SYSTEM_IDLE_CPU_PCT[kd]
        for kd in kds
    }

    sorted_kds = sorted(kds, key=lambda k: efficiency[k], reverse=True)
    x = np.arange(len(sorted_kds))
    values = [efficiency[kd] for kd in sorted_kds]
    colors = [KD_COLORS[kd] for kd in sorted_kds]

    bars = ax.bar(x, values, color=colors, edgecolor="white", linewidth=0.5)

    # Add value labels
    for bar, val, kd in zip(bars, values, sorted_kds):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 200,
                f"{val:.0f}", ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(sorted_kds)
    ax.set_ylabel("Throughput / CPU Overhead\n(ops/s per %CPU)")
    ax.set_title("Data-Plane Efficiency per Unit of System Overhead")

    # Highlight the efficiency gap
    best = max(values)
    worst = min(values)
    ax.annotate(f"{best / worst:.0f}× gap",
                xy=(0.5, best * 0.7), fontsize=11, fontweight="bold",
                ha="center", color="#333333")

    fig.tight_layout()
    return save_figure(fig, "proposition_p10_efficiency", FIGURES_DIR)


def plot_security_maintainability():
    """
    H5: Scatter plot — CIS security score vs setup time.
    Shows security compliance burden correlates with setup complexity.
    """
    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    kds = KD_ORDER
    for kd in kds:
        sec = SECURITY_SCORES[kd]
        setup = SETUP_HOURS[kd]
        color = KD_COLORS[kd]

        ax.scatter(sec, setup, c=color, s=120, zorder=5,
                   edgecolors="black", linewidths=0.5)
        ax.annotate(kd, (sec, setup), textcoords="offset points",
                     xytext=(8, 5), fontsize=10)

    # Trend line
    secs = [SECURITY_SCORES[kd] for kd in kds]
    setups = [SETUP_HOURS[kd] for kd in kds]
    z = np.polyfit(secs, setups, 1)
    p = np.poly1d(z)
    x_fit = np.linspace(0, 60, 100)
    ax.plot(x_fit, p(x_fit), "--", color="gray", alpha=0.5, linewidth=1)

    ax.set_xlabel("CIS Security Score (%)")
    ax.set_ylabel("Setup Time (hours)")
    ax.set_title("Security Compliance vs Setup Complexity (ρ=0.92, p=0.03)")

    # Highlight lightweight zone
    ax.axhspan(0, 4, alpha=0.08, color="green", label="Easy setup (<4h)")
    ax.axhspan(10, 16, alpha=0.08, color="red", label="Complex setup (>10h)")
    ax.legend(fontsize=8, loc="upper left")

    fig.tight_layout()
    return save_figure(fig, "proposition_p12_security_maintainability", FIGURES_DIR)


def plot_proposition_summary():
    """
    Summary heatmap showing all tested propositions and their verdicts.
    """
    json_path = os.path.join(RESULTS_DIR, "proposition_validation.json")
    if not os.path.exists(json_path):
        print("  Missing proposition_validation.json")
        return

    import json
    with open(json_path) as f:
        results = json.load(f)

    fig, ax = plt.subplots(figsize=(7, 3.5))

    props = [r["proposition_id"] for r in results]
    directions = [r["direction"] for r in results]
    strengths = [r["strength"] for r in results]

    # Map strength to numeric value for color
    strength_map = {"strong": 1.0, "moderate": 0.6, "weak": 0.3, "inconclusive": 0.0}
    strength_vals = [strength_map.get(s, 0) for s in strengths]

    y = np.arange(len(props))
    colors = plt.cm.RdYlGn([0.3 + 0.5 * v for v in strength_vals])

    bars = ax.barh(y, strength_vals, color=colors, edgecolor="white", linewidth=0.5)

    # Add labels
    for i, (bar, prop, direction, strength) in enumerate(
        zip(bars, props, directions, strengths)
    ):
        ax.text(0.02, bar.get_y() + bar.get_height() / 2,
                f"{prop}: {direction}",
                va="center", fontsize=9, fontweight="bold")
        ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                strength.upper(), va="center", fontsize=9, color="#333333")

    ax.set_yticks([])
    ax.set_xlim(0, 1.3)
    ax.set_xlabel("Evidence Strength")
    ax.set_title("Trade-off Hypothesis Validation Summary")
    ax.set_xticks([0, 0.3, 0.6, 1.0])
    ax.set_xticklabels(["None", "Weak", "Moderate", "Strong"])

    fig.tight_layout()
    return save_figure(fig, "proposition_validation_summary", FIGURES_DIR)


if __name__ == "__main__":
    setup_style()

    print("Generating proposition validation figures...")

    plot_security_vs_resources()
    plot_startup_latency_comparison()
    plot_efficiency_comparison()
    plot_security_maintainability()
    plot_proposition_summary()

    print("\nDone.")
