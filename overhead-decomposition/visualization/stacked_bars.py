#!/usr/bin/env python3
"""
Generate stacked bar charts for per-container overhead decomposition.

Figures:
1. Idle CPU decomposition per pod (stacked bar)
2. Idle Memory decomposition per pod (stacked bar)
3. System vs Workload comparison across test types (grouped bars)
4. Orchestration tax across test types
"""

import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import FIGURES_DIR, RESULTS_DIR
from visualization.style import (
    FIGURE_SIZE,
    FIGURE_SIZE_WIDE,
    POD_COLORS,
    QOS_LABELS,
    ROLE_COLORS,
    TEST_TYPE_LABELS,
    save_figure,
    setup_style,
)


def plot_idle_cpu_decomposition(kd: str = "k0s"):
    """
    Stacked bar chart: per-pod CPU usage during idle.

    Shows each system pod's contribution to total idle CPU overhead.
    """
    pivot_path = os.path.join(RESULTS_DIR, f"{kd}_idle_overhead_pivot.csv")
    if not os.path.exists(pivot_path):
        print(f"  Missing: {pivot_path}")
        return

    df = pd.read_csv(pivot_path)

    # Sort by CPU (descending)
    if "cpu_pct" not in df.columns:
        print("  No cpu_pct column in pivot data")
        return

    df = df.sort_values("cpu_pct", ascending=True)

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    labels = [f"Pod {l}...\n({QOS_LABELS.get(q, q)})"
              for l, q in zip(df["pod_label"], df["qos_class"])]
    y_pos = np.arange(len(df))
    bars = ax.barh(y_pos, df["cpu_pct"], color=POD_COLORS[:len(df)], edgecolor="white", linewidth=0.5)

    # Add value labels
    for bar, val in zip(bars, df["cpu_pct"]):
        ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}%", va="center", fontsize=9)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.set_xlabel("CPU Usage (%)")
    ax.set_title(f"{kd} Idle: Per-Container CPU on Worker (RPi4)")

    total = df["cpu_pct"].sum()
    ax.axvline(total, color="black", linestyle="--", linewidth=0.8, alpha=0.5)
    ax.text(total + 0.02, len(df) - 0.5, f"Total: {total:.3f}%",
            fontsize=9, fontstyle="italic")

    fig.tight_layout()
    return save_figure(fig, f"{kd}_idle_cpu_decomposition", FIGURES_DIR)


def plot_idle_memory_decomposition(kd: str = "k0s"):
    """
    Stacked bar chart: per-pod memory usage during idle.
    """
    pivot_path = os.path.join(RESULTS_DIR, f"{kd}_idle_overhead_pivot.csv")
    if not os.path.exists(pivot_path):
        return

    df = pd.read_csv(pivot_path)
    if "mem_mib" not in df.columns:
        return

    df = df.sort_values("mem_mib", ascending=True)

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    labels = [f"Pod {l}...\n({QOS_LABELS.get(q, q)})"
              for l, q in zip(df["pod_label"], df["qos_class"])]
    y_pos = np.arange(len(df))
    bars = ax.barh(y_pos, df["mem_mib"], color=POD_COLORS[:len(df)], edgecolor="white", linewidth=0.5)

    for bar, val in zip(bars, df["mem_mib"]):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f} MiB", va="center", fontsize=9)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Memory Usage (MiB)")
    ax.set_title(f"{kd} Idle: Per-Container Memory on Worker (RPi4)")

    total = df["mem_mib"].sum()
    ax.axvline(total, color="black", linestyle="--", linewidth=0.8, alpha=0.5)
    ax.text(total + 0.3, len(df) - 0.5, f"Total: {total:.1f} MiB",
            fontsize=9, fontstyle="italic")

    fig.tight_layout()
    return save_figure(fig, f"{kd}_idle_memory_decomposition", FIGURES_DIR)


def plot_system_vs_workload(kd: str = "k0s"):
    """
    Grouped bar chart: system vs workload overhead across test types.
    Shows how system overhead changes from idle to loaded tests.
    """
    loaded_path = os.path.join(RESULTS_DIR, f"{kd}_loaded_overhead.csv")
    if not os.path.exists(loaded_path):
        return

    df = pd.read_csv(loaded_path)

    # CPU comparison
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=FIGURE_SIZE_WIDE)

    test_types = ["idle", "cp_heavy_12client", "dp_redis_density"]
    x = np.arange(len(test_types))
    width = 0.35

    for metric, ax, ylabel, title_suffix in [
        ("cpu_pct", ax1, "CPU Usage (%)", "CPU"),
        ("mem_mib", ax2, "Memory (MiB)", "Memory"),
    ]:
        metric_data = df[df["metric"] == metric]

        sys_vals = []
        wl_vals = []
        sys_errs = []
        wl_errs = []

        for tt in test_types:
            tt_data = metric_data[metric_data["test_type"] == tt]
            sys_row = tt_data[tt_data["role"] == "system"]
            wl_row = tt_data[tt_data["role"] == "workload"]

            sys_vals.append(sys_row["mean"].iloc[0] if len(sys_row) > 0 else 0)
            sys_errs.append(sys_row["std"].iloc[0] if len(sys_row) > 0 else 0)
            wl_vals.append(wl_row["mean"].iloc[0] if len(wl_row) > 0 else 0)
            wl_errs.append(wl_row["std"].iloc[0] if len(wl_row) > 0 else 0)

        bars1 = ax.bar(x - width / 2, sys_vals, width, yerr=sys_errs,
                       label="System", color=ROLE_COLORS["system"],
                       capsize=3, edgecolor="white")
        bars2 = ax.bar(x + width / 2, wl_vals, width, yerr=wl_errs,
                       label="Workload", color=ROLE_COLORS["workload"],
                       capsize=3, edgecolor="white")

        ax.set_xticks(x)
        ax.set_xticklabels([TEST_TYPE_LABELS.get(t, t) for t in test_types])
        ax.set_ylabel(ylabel)
        ax.set_title(f"{title_suffix} Overhead")
        ax.legend()

        # Add value labels on system bars
        for bar, val in zip(bars1, sys_vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        f"{val:.1f}", ha="center", va="bottom", fontsize=8)

    fig.suptitle(f"{kd}: System vs Workload Overhead on Worker (RPi4)", fontsize=13)
    fig.tight_layout()
    return save_figure(fig, f"{kd}_system_vs_workload", FIGURES_DIR)


def plot_orchestration_tax(kd: str = "k0s"):
    """
    Bar chart: orchestration tax (% of node capacity consumed by system overhead).
    """
    tax_path = os.path.join(RESULTS_DIR, f"{kd}_orchestration_tax.csv")
    if not os.path.exists(tax_path):
        return

    df = pd.read_csv(tax_path)

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    test_types = df["test_type"].unique()
    x = np.arange(len(test_types))
    width = 0.35

    cpu_tax = []
    mem_tax = []
    for tt in test_types:
        tt_data = df[df["test_type"] == tt]
        cpu_row = tt_data[tt_data["metric"] == "cpu_pct"]
        mem_row = tt_data[tt_data["metric"] == "mem_mib"]
        cpu_tax.append(cpu_row["orchestration_tax_pct"].iloc[0] if len(cpu_row) > 0 else 0)
        mem_tax.append(mem_row["orchestration_tax_pct"].iloc[0] if len(mem_row) > 0 else 0)

    bars1 = ax.bar(x - width / 2, cpu_tax, width, label="CPU Tax",
                   color="#e41a1c", edgecolor="white")
    bars2 = ax.bar(x + width / 2, mem_tax, width, label="Memory Tax",
                   color="#377eb8", edgecolor="white")

    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, height,
                    f"{height:.2f}%", ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels([TEST_TYPE_LABELS.get(t, t) for t in test_types])
    ax.set_ylabel("% of Node Capacity")
    ax.set_title(f"{kd}: Orchestration Tax on Worker (RPi4)")
    ax.legend()

    fig.tight_layout()
    return save_figure(fig, f"{kd}_orchestration_tax", FIGURES_DIR)


def plot_system_overhead_growth(kd: str = "k0s"):
    """
    Bar chart showing how system overhead grows from idle to loaded.
    """
    loaded_path = os.path.join(RESULTS_DIR, f"{kd}_loaded_overhead.csv")
    if not os.path.exists(loaded_path):
        return

    df = pd.read_csv(loaded_path)
    cpu_sys = df[(df["metric"] == "cpu_pct") & (df["role"] == "system")]

    if cpu_sys.empty:
        return

    fig, ax = plt.subplots(figsize=FIGURE_SIZE)

    test_types = ["idle", "cp_heavy_12client", "dp_redis_density"]
    values = []
    errors = []
    labels = []

    for tt in test_types:
        row = cpu_sys[cpu_sys["test_type"] == tt]
        if len(row) > 0:
            values.append(row["mean"].iloc[0])
            errors.append(row["std"].iloc[0])
            labels.append(TEST_TYPE_LABELS.get(tt, tt))

    x = np.arange(len(labels))
    colors = ["#4daf4a", "#e41a1c", "#377eb8"]
    bars = ax.bar(x, values, yerr=errors, capsize=5,
                  color=colors[:len(values)], edgecolor="white")

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{val:.2f}%", ha="center", va="bottom", fontsize=10)

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("System CPU Usage (%)")
    ax.set_title(f"{kd}: System Overhead Growth Under Load (RPi4 Worker)")

    # Add idle baseline line
    if values:
        idle_val = values[0]
        ax.axhline(idle_val, color="gray", linestyle="--", linewidth=0.8, alpha=0.5)

    fig.tight_layout()
    return save_figure(fig, f"{kd}_system_overhead_growth", FIGURES_DIR)


if __name__ == "__main__":
    setup_style()

    print("Generating visualizations...")

    plot_idle_cpu_decomposition()
    plot_idle_memory_decomposition()
    plot_system_vs_workload()
    plot_orchestration_tax()
    plot_system_overhead_growth()

    print("\nDone.")
