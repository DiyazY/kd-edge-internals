#!/usr/bin/env python3
"""
System vs workload overhead decomposition under load for k0s.

Reads the silver-layer parquet, splits pods into system vs workload
using the classification, and computes:
1. Per-role (system/workload) aggregated overhead per test type
2. Orchestration tax: system overhead / total node capacity

Reads from: data/silver/{kd}_cgroup_metrics.parquet
             data/pod_classification.json
Output:      results/{kd}_loaded_overhead.csv
             results/{kd}_orchestration_tax.csv
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from classification.classify_pods import load_classification
from config import NODE_CPU_CAPACITY, NODE_RAM_MB, RESULTS_DIR, SILVER_DIR


def loaded_decomposition(kd: str = "k0s") -> pd.DataFrame:
    """
    Compute system vs workload overhead for all test types.

    Reads silver parquet, adds classification (system/workload role),
    then for each (test_type, role, metric): sums across pods per run,
    then computes mean ± std across runs.
    """
    silver_path = os.path.join(SILVER_DIR, f"{kd}_cgroup_metrics.parquet")
    if not os.path.exists(silver_path):
        print(f"Silver file not found: {silver_path}")
        return pd.DataFrame()

    df = pd.read_parquet(silver_path)

    # Add classification
    pod_map = load_classification()
    df["role"] = df["pod_uuid"].map(
        lambda u: pod_map.get(u, {}).get("role", "unknown")
    )

    # Print summary per test type
    for test_type in df["test_type"].unique():
        tt_data = df[df["test_type"] == test_type]
        n_pods = tt_data["pod_uuid"].nunique()
        n_system = tt_data[tt_data["role"] == "system"]["pod_uuid"].nunique()
        n_workload = tt_data[tt_data["role"] == "workload"]["pod_uuid"].nunique()
        print(f"  {test_type}: {n_pods} pods ({n_system} system, {n_workload} workload)")

    # For each (test_type, role, metric, run): sum values across pods,
    # then compute mean ± std across runs.
    summaries = []

    for (test_type, role, metric), group in df.groupby(
        ["test_type", "role", "metric"]
    ):
        if role == "unknown":
            continue

        per_run = group.groupby("run")["value"].sum()

        summaries.append({
            "kd": kd,
            "test_type": test_type,
            "role": role,
            "metric": metric,
            "mean": per_run.mean(),
            "std": per_run.std(),
            "n_runs": len(per_run),
            "n_pods": group["pod_uuid"].nunique(),
        })

    return pd.DataFrame(summaries)


def compute_orchestration_tax(summary: pd.DataFrame) -> pd.DataFrame:
    """Compute orchestration tax: system overhead / total node capacity."""
    cpu_cap = NODE_CPU_CAPACITY.get("node_2", 400)
    ram_cap = NODE_RAM_MB.get("node_2", 8192)

    tax_rows = []
    for _, row in summary[summary["role"] == "system"].iterrows():
        if row["metric"] == "cpu_pct":
            tax_pct = row["mean"] / cpu_cap * 100
        elif row["metric"] == "mem_mib":
            tax_pct = row["mean"] / ram_cap * 100
        else:
            continue

        tax_rows.append({
            "test_type": row["test_type"],
            "metric": row["metric"],
            "system_overhead": row["mean"],
            "node_capacity": cpu_cap if row["metric"] == "cpu_pct" else ram_cap,
            "orchestration_tax_pct": tax_pct,
        })

    return pd.DataFrame(tax_rows)


def print_loaded_report(summary: pd.DataFrame, tax: pd.DataFrame):
    """Print a formatted loaded overhead report."""
    print(f"\n{'=' * 70}")
    print(f"  SYSTEM vs WORKLOAD OVERHEAD COMPARISON")
    print(f"{'=' * 70}")

    for test_type in ["idle", "cp_heavy_12client", "dp_redis_density"]:
        test_data = summary[summary["test_type"] == test_type]
        if test_data.empty:
            continue

        print(f"\n  [{test_type}]")

        for metric_name in ["cpu_pct", "mem_mib"]:
            metric_data = test_data[test_data["metric"] == metric_name]
            if metric_data.empty:
                continue

            unit = "%" if metric_name == "cpu_pct" else "MiB"
            label = "CPU" if metric_name == "cpu_pct" else "Memory"
            print(f"    {label}:")

            for _, row in metric_data.iterrows():
                icon = "🔧" if row["role"] == "system" else "📦"
                print(
                    f"      {icon} {row['role']:10s}: "
                    f"{row['mean']:8.3f} {unit} ± {row['std']:6.3f} "
                    f"({row['n_pods']} pods, {row['n_runs']} runs)"
                )

    if not tax.empty:
        print(f"\n  {'─' * 60}")
        print(f"  ORCHESTRATION TAX (system overhead / node capacity)")
        for _, row in tax.iterrows():
            label = "CPU" if row["metric"] == "cpu_pct" else "Memory"
            print(
                f"    {row['test_type']:25s} {label:8s}: "
                f"{row['orchestration_tax_pct']:.3f}%"
            )


def save_loaded_results(summary: pd.DataFrame, tax: pd.DataFrame, kd: str = "k0s"):
    """Save loaded overhead results."""
    os.makedirs(RESULTS_DIR, exist_ok=True)

    summary_path = os.path.join(RESULTS_DIR, f"{kd}_loaded_overhead.csv")
    summary.to_csv(summary_path, index=False, float_format="%.6f")
    print(f"\n  Saved: {summary_path}")

    tax_path = os.path.join(RESULTS_DIR, f"{kd}_orchestration_tax.csv")
    tax.to_csv(tax_path, index=False, float_format="%.6f")
    print(f"  Saved: {tax_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Loaded overhead decomposition")
    parser.add_argument("--kd", default="k0s", help="KD to analyze")
    args = parser.parse_args()

    print(f"Loaded decomposition for {args.kd}...")
    summary = loaded_decomposition(kd=args.kd)

    if not summary.empty:
        tax = compute_orchestration_tax(summary)
        print_loaded_report(summary, tax)
        save_loaded_results(summary, tax, kd=args.kd)
