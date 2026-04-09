#!/usr/bin/env python3
"""
DuckDB-based data loading from shared parquets.

Replaces the old MongoDB → CSV extraction pipeline. Reads per-run parquets:
    data/raw/{kd}/{test_type}_run{N}.parquet

Column renaming (parquet → analysis convention):
    relative_time → timestamp
    metric_name   → name
    metric_id     → id
"""

import os

import duckdb
import pandas as pd

from config import NUM_RUNS, PARQUET_ROOT


def _parquet_path(kd: str, test_type: str, run_num: int) -> str:
    """Build path to a per-run parquet file."""
    return os.path.join(PARQUET_ROOT, kd, f"{test_type}_run{run_num}.parquet")


def load_cgroup_run(
    kd: str,
    test_type: str,
    run_num: int,
    hostname: str | None = None,
) -> pd.DataFrame:
    """
    Load k8s.cgroup.* data for a single run from parquet.

    Returns DataFrame with columns matching the old CSV convention:
        hostname, chart_id, chart_context, chart_family,
        id, name, value, units, timestamp
    """
    path = _parquet_path(kd, test_type, run_num)
    if not os.path.exists(path):
        return pd.DataFrame()

    conditions = ["chart_context LIKE 'k8s.cgroup.%'"]
    if hostname:
        conditions.append(f"hostname = '{hostname}'")

    where = " AND ".join(conditions)

    query = f"""
        SELECT
            relative_time AS timestamp,
            hostname,
            chart_context,
            chart_id,
            chart_family,
            metric_id AS id,
            metric_name AS name,
            value,
            units
        FROM '{path}'
        WHERE {where}
        ORDER BY timestamp, hostname, chart_context, name
    """

    con = duckdb.connect()
    df = con.execute(query).fetchdf()
    con.close()
    return df


def load_all_cgroup_runs(
    kd: str,
    test_type: str,
    hostname: str | None = None,
) -> list[tuple[int, pd.DataFrame]]:
    """
    Load k8s.cgroup.* data for all available runs (up to NUM_RUNS).

    Returns list of (run_num, DataFrame) tuples.
    Skips runs whose parquet files don't exist.
    """
    runs = []
    for run_num in range(1, NUM_RUNS + 1):
        df = load_cgroup_run(kd, test_type, run_num, hostname)
        if not df.empty:
            runs.append((run_num, df))
    return runs
