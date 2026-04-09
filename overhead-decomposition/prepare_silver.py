#!/usr/bin/env python3
"""
Prepare silver-layer cgroup metrics from raw parquets.

Reads raw per-run parquets from data/raw/,
applies all data wrangling (chart_id parsing, dimension filtering,
temporal aggregation), and outputs clean per-pod per-metric per-run
values to data/silver/.

This is the single entry point for all data cleaning. Downstream
analysis scripts read only from the silver parquet.

Output: data/silver/{kd}_cgroup_metrics.parquet
Columns: kd, test_type, run, hostname, pod_uuid, qos_class,
         metric, value, units, n_samples
"""

import os
import re
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import NUM_RUNS, SILVER_DIR, TEST_TYPES
from data_loading import load_cgroup_run

# ---------------------------------------------------------------------------
# Metric definitions: which cgroup contexts to extract and how to aggregate.
#
# Each entry maps a Netdata chart_context to:
#   name     — short metric name used in downstream analysis
#   units    — display units (Netdata-native: MiB for mem, kbit/s for net)
#   dims     — which dimension names to keep (None = all)
#   dim_agg  — how to combine dimensions at each timestamp ("sum" or "max")
# ---------------------------------------------------------------------------
KEY_METRICS = {
    "k8s.cgroup.cpu": {
        "name": "cpu_pct", "units": "%",
        "dims": ["user", "system"], "dim_agg": "sum",
    },
    "k8s.cgroup.mem_usage": {
        "name": "mem_mib", "units": "MiB",
        "dims": ["ram"], "dim_agg": "max",
    },
    "k8s.cgroup.mem_utilization": {
        "name": "mem_util_pct", "units": "%",
        "dims": None, "dim_agg": "max",
    },
    "k8s.cgroup.net_net": {
        "name": "net_kbps", "units": "kbit/s",
        "dims": ["received", "sent"], "dim_agg": "sum",
    },
    "k8s.cgroup.io": {
        "name": "io_kibs", "units": "KiB/s",
        "dims": ["read", "write"], "dim_agg": "sum",
    },
    "k8s.cgroup.pgfaults": {
        "name": "pgfaults", "units": "faults/s",
        "dims": ["pgfault", "pgmajfault"], "dim_agg": "sum",
    },
    "k8s.cgroup.throttled": {
        "name": "throttled", "units": "events/s",
        "dims": ["throttled"], "dim_agg": "sum",
    },
    "k8s.cgroup.cpu_some_pressure": {
        "name": "cpu_pressure", "units": "%",
        "dims": ["some 10"], "dim_agg": "max",
    },
    "k8s.cgroup.memory_some_pressure": {
        "name": "mem_pressure", "units": "%",
        "dims": ["some 10"], "dim_agg": "max",
    },
}

# Regex for parsing cgroup chart_id:
#   cgroup_k8s_kubepods_{qos}_{pod_uuid}_{container_hash}.{metric_suffix}
_CHART_ID_RE = re.compile(
    r"cgroup_k8s_kubepods_(\w+)_(pod[0-9a-f-]+)_[0-9a-f]+\..+"
)


def _parse_pod_info(chart_id: str) -> tuple[str | None, str | None]:
    """Extract (pod_uuid, qos_class) from a cgroup chart_id."""
    m = _CHART_ID_RE.match(chart_id)
    if m:
        return m.group(2), m.group(1)
    return None, None


def _compute_pod_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute time-averaged metrics per pod from a parsed cgroup DataFrame.

    For multi-dimension metrics (e.g., CPU has user + system):
    1. Filter to specified dimensions
    2. Aggregate dimensions at each timestamp (sum or max)
    3. Average across timestamps → one scalar per pod per metric
    """
    results = []

    for (hostname, pod_uuid, qos_class, context), group in df.groupby(
        ["hostname", "pod_uuid", "qos_class", "chart_context"]
    ):
        if context not in KEY_METRICS:
            continue

        info = KEY_METRICS[context]
        dims = info.get("dims")

        filtered = group[group["name"].isin(dims)] if dims else group
        if filtered.empty:
            continue

        agg_fn = "sum" if info.get("dim_agg", "sum") == "sum" else "max"
        ts_values = filtered.groupby("timestamp")["value"].agg(agg_fn)

        results.append({
            "hostname": hostname,
            "pod_uuid": pod_uuid,
            "qos_class": qos_class,
            "metric": info["name"],
            "value": float(ts_values.mean()),
            "units": info["units"],
            "n_samples": len(ts_values),
        })

    return pd.DataFrame(results)


def prepare_silver(kd: str = "k0s") -> str | None:
    """
    Extract and clean cgroup metrics for all test types and runs.

    Reads raw parquets, parses chart_ids, aggregates dimensions and
    timestamps, and saves one silver parquet per KD.
    """
    os.makedirs(SILVER_DIR, exist_ok=True)

    all_rows = []

    for test_type in TEST_TYPES:
        for run_num in range(1, NUM_RUNS + 1):
            df = load_cgroup_run(kd, test_type, run_num)
            if df.empty:
                continue

            # Parse pod_uuid and qos_class from chart_id
            parsed = df["chart_id"].apply(_parse_pod_info)
            df = df.copy()
            df["pod_uuid"] = parsed.apply(lambda x: x[0])
            df["qos_class"] = parsed.apply(lambda x: x[1])
            df = df.dropna(subset=["pod_uuid"])

            if df.empty:
                continue

            metrics = _compute_pod_metrics(df)
            if metrics.empty:
                continue

            metrics["kd"] = kd
            metrics["test_type"] = test_type
            metrics["run"] = run_num
            all_rows.append(metrics)

            n_pods = metrics["pod_uuid"].nunique()
            print(
                f"  {kd}/{test_type}/run{run_num}: "
                f"{n_pods} pods, {len(metrics)} metric rows"
            )

    if not all_rows:
        print(f"No cgroup data found for {kd}")
        return None

    silver = pd.concat(all_rows, ignore_index=True)

    # Canonical column order
    silver = silver[[
        "kd", "test_type", "run", "hostname", "pod_uuid",
        "qos_class", "metric", "value", "units", "n_samples",
    ]]

    out_path = os.path.join(SILVER_DIR, f"{kd}_cgroup_metrics.parquet")
    silver.to_parquet(out_path, index=False)

    n_pods = silver["pod_uuid"].nunique()
    print(f"\nSilver saved: {out_path}")
    print(f"  {len(silver)} rows, {n_pods} unique pods")
    for tt in TEST_TYPES:
        tt_data = silver[silver["test_type"] == tt]
        if not tt_data.empty:
            n_runs = tt_data["run"].nunique()
            print(f"  {tt}: {n_runs} runs, {tt_data['pod_uuid'].nunique()} pods")

    return out_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Prepare silver cgroup metrics")
    parser.add_argument("--kd", default="k0s", help="KD to process")
    args = parser.parse_args()

    print(f"Preparing silver layer for {args.kd}...")
    prepare_silver(kd=args.kd)
