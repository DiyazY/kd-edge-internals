#!/usr/bin/env python3
"""
Per-container idle overhead decomposition for k0s.

Reads the silver-layer parquet (pre-aggregated per-pod per-metric per-run
values), filters to idle test runs, and computes mean ± std across 5 runs
for each system pod.

Reads from: data/silver/{kd}_cgroup_metrics.parquet
             data/pod_classification.json
Output:      results/{kd}_idle_overhead.csv
             results/{kd}_idle_overhead_pivot.csv
             results/{kd}_idle_overhead_totals.csv
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from classification.classify_pods import load_classification
from config import NODE_CPU_CAPACITY, NODE_RAM_MB, RESULTS_DIR, SILVER_DIR


def idle_decomposition(kd: str = "k0s") -> pd.DataFrame:
    """
    Compute per-container idle overhead across all idle runs.

    Reads silver parquet, filters to idle test type, then computes
    mean ± std across runs for each (pod_uuid, metric) pair.
    """
    silver_path = os.path.join(SILVER_DIR, f"{kd}_cgroup_metrics.parquet")
    if not os.path.exists(silver_path):
        print(f"Silver file not found: {silver_path}")
        return pd.DataFrame()

    df = pd.read_parquet(silver_path)
    idle = df[df["test_type"] == "idle"]

    if idle.empty:
        print(f"No idle data in silver for {kd}")
        return pd.DataFrame()

    n_runs = idle["run"].nunique()
    print(f"Processing {n_runs} idle runs for {kd}...")

    # Mean ± std across runs for each pod × metric
    summary = (
        idle
        .groupby(["hostname", "pod_uuid", "qos_class", "metric", "units"])
        .agg(
            mean=("value", "mean"),
            std=("value", "std"),
            n_runs=("value", "count"),
        )
        .reset_index()
    )

    # Add classification
    pod_map = load_classification()
    summary["role"] = summary["pod_uuid"].map(
        lambda u: pod_map.get(u, {}).get("role", "unknown")
    )

    # Short pod label (first 8 chars of UUID)
    summary["pod_label"] = summary["pod_uuid"].apply(
        lambda x: x.replace("pod", "")[:8]
    )

    return summary


def print_idle_report(summary: pd.DataFrame, kd: str = "k0s"):
    """Print a formatted idle overhead report."""
    if summary.empty:
        print("No data to report.")
        return

    print(f"\n{'=' * 70}")
    print(f"  IDLE OVERHEAD DECOMPOSITION: {kd} (node_2 / RPi4)")
    print(f"{'=' * 70}")

    # Per-pod CPU
    cpu_data = summary[summary["metric"] == "cpu_pct"].sort_values(
        "mean", ascending=False
    )
    if not cpu_data.empty:
        print(f"\n  CPU Usage (percentage of one core):")
        total_cpu = 0
        for _, row in cpu_data.iterrows():
            print(
                f"    Pod {row['pod_label']}... ({row['qos_class']:12s}): "
                f"{row['mean']:6.3f}% ± {row['std']:5.3f}%"
            )
            total_cpu += row["mean"]
        capacity = NODE_CPU_CAPACITY.get("node_2", 400)
        print(f"    {'─' * 50}")
        print(
            f"    Total system CPU:  {total_cpu:.3f}%  "
            f"({total_cpu / capacity * 100:.2f}% of node capacity)"
        )

    # Per-pod Memory
    mem_data = summary[summary["metric"] == "mem_mib"].sort_values(
        "mean", ascending=False
    )
    if not mem_data.empty:
        print(f"\n  Memory Usage (MiB):")
        total_mem = 0
        for _, row in mem_data.iterrows():
            print(
                f"    Pod {row['pod_label']}... ({row['qos_class']:12s}): "
                f"{row['mean']:8.2f} MiB ± {row['std']:6.2f} MiB"
            )
            total_mem += row["mean"]
        node_ram = NODE_RAM_MB.get("node_2", 8192)
        print(f"    {'─' * 50}")
        print(
            f"    Total system RAM:  {total_mem:.2f} MiB  "
            f"({total_mem / node_ram * 100:.2f}% of {node_ram} MiB)"
        )

    print()


def save_results(summary: pd.DataFrame, kd: str = "k0s"):
    """Save idle overhead results to CSV."""
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Full per-pod detail
    detail_path = os.path.join(RESULTS_DIR, f"{kd}_idle_overhead.csv")
    summary.to_csv(detail_path, index=False, float_format="%.6f")
    print(f"  Saved detail: {detail_path}")

    # Pivot table: pods × metrics
    pivot = summary.pivot_table(
        index=["hostname", "pod_uuid", "pod_label", "role", "qos_class"],
        columns="metric",
        values="mean",
    ).reset_index()

    pivot_path = os.path.join(RESULTS_DIR, f"{kd}_idle_overhead_pivot.csv")
    pivot.to_csv(pivot_path, index=False, float_format="%.6f")
    print(f"  Saved pivot: {pivot_path}")

    # Summary totals
    totals = summary.groupby("metric").agg(
        total_mean=("mean", "sum"),
        total_std=("std", lambda x: np.sqrt((x**2).sum())),
    ).reset_index()

    totals_path = os.path.join(RESULTS_DIR, f"{kd}_idle_overhead_totals.csv")
    totals.to_csv(totals_path, index=False, float_format="%.6f")
    print(f"  Saved totals: {totals_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute idle overhead decomposition")
    parser.add_argument("--kd", default="k0s", help="KD to analyze (default: k0s)")
    args = parser.parse_args()

    summary = idle_decomposition(kd=args.kd)

    if not summary.empty:
        print_idle_report(summary, kd=args.kd)
        save_results(summary, kd=args.kd)
    else:
        print("No idle overhead data to analyze.")
