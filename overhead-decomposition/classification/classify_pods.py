#!/usr/bin/env python3
"""
Classify pods as system components or workload using the silver layer.

Strategy (idle-test enumeration):
1. All pods present during idle tests = system (no workloads deployed)
2. Pods appearing only in loaded tests = workload (k-bench test pods)

Reads from: data/silver/{kd}_cgroup_metrics.parquet
Output:      data/pod_classification.json
"""

import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_DIR, SILVER_DIR


def load_classification() -> dict:
    """Load pod classification and return mapping from pod_uuid to entry."""
    path = os.path.join(DATA_DIR, "pod_classification.json")
    with open(path) as f:
        classifications = json.load(f)
    return {c["pod_uuid"]: c for c in classifications}


def classify_pods(kd: str = "k0s") -> list[dict]:
    """
    Classify all pods for a given KD using idle-vs-loaded differential.

    Reads the silver parquet to determine which pod_uuids appear in
    which test types, then applies the idle enumeration rule.
    """
    silver_path = os.path.join(SILVER_DIR, f"{kd}_cgroup_metrics.parquet")
    if not os.path.exists(silver_path):
        print(f"Silver file not found: {silver_path}")
        print("Run prepare_silver.py first.")
        return []

    df = pd.read_parquet(silver_path)

    # Build pod inventory: unique (hostname, pod_uuid) per test_type
    pod_info = (
        df.groupby(["hostname", "pod_uuid"])
        .agg(
            qos_class=("qos_class", "first"),
            test_types=("test_type", lambda x: sorted(x.unique())),
            n_runs=("run", "nunique"),
            contexts=("metric", lambda x: sorted(x.unique())),
        )
        .reset_index()
    )

    # Idle pods = system, loaded-only pods = workload
    idle_pods = set(
        df[df["test_type"] == "idle"]["pod_uuid"].unique()
    )
    loaded_test_types = [t for t in df["test_type"].unique() if t != "idle"]

    classifications = []
    for _, row in pod_info.iterrows():
        is_idle = row["pod_uuid"] in idle_pods
        found_in_loaded = [t for t in row["test_types"] if t != "idle"]

        classifications.append({
            "kd": kd,
            "hostname": row["hostname"],
            "pod_uuid": row["pod_uuid"],
            "qos_class": row["qos_class"],
            "role": "system" if is_idle else "workload",
            "classification_method": "idle_enumeration" if is_idle else "differential_analysis",
            "found_in_idle": is_idle,
            "found_in_loaded": found_in_loaded,
            "contexts": row["contexts"],
        })

    # Summary
    system_count = sum(1 for c in classifications if c["role"] == "system")
    workload_count = sum(1 for c in classifications if c["role"] == "workload")

    print(f"\n  System pods: {system_count}")
    print(f"  Workload pods: {workload_count}")
    print(f"  Total classified: {len(classifications)}")

    for c in classifications:
        icon = "🔧" if c["role"] == "system" else "📦"
        loaded = ", ".join(c["found_in_loaded"]) or "none"
        print(
            f"    {icon} {c['hostname']:10s} {c['qos_class']:12s} "
            f"{c['pod_uuid'][:35]}... "
            f"role={c['role']:8s} loaded=[{loaded}]"
        )

    return classifications


def save_classification(
    classifications: list[dict],
    output_path: str | None = None,
) -> str:
    """Save classifications to JSON."""
    if output_path is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        output_path = os.path.join(DATA_DIR, "pod_classification.json")

    with open(output_path, "w") as f:
        json.dump(classifications, f, indent=2)

    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Classify pods as system or workload")
    parser.add_argument("--kd", default="k0s", help="KD to classify")
    parser.add_argument("--output", "-o", help="Output JSON path")
    args = parser.parse_args()

    print(f"Classifying pods for {args.kd}...")
    classifications = classify_pods(kd=args.kd)

    if classifications:
        path = save_classification(classifications, args.output)
        print(f"\nSaved to: {path}")
    else:
        print("\nNo classifications produced.")
