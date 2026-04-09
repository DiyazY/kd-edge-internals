"""
Data loading utilities for energy analysis.

Reads per-run parquets from data/raw/ via DuckDB,
renaming columns to match the schema expected by analysis scripts:
    metric_id   → id
    metric_name → name
    relative_time → timestamp
"""

import os

import duckdb
import pandas as pd

from config import KD_ORDER, NUM_RUNS, PARQUET_ROOT, TEST_TYPES


def load_run(
    kd: str,
    test_type: str,
    run_num: int,
    contexts: list[str] | None = None,
    hostname: str | None = None,
) -> pd.DataFrame:
    """
    Load a single run's parquet with column renaming.

    Parameters
    ----------
    kd : str
        Kubernetes distribution (e.g., "k0s").
    test_type : str
        Test type (e.g., "idle", "cp_heavy_12client").
    run_num : int
        Run number (1-5).
    contexts : list[str], optional
        Filter to these chart_context values (e.g., ["cpufreq.cpufreq"]).
    hostname : str, optional
        Filter to a specific hostname.

    Returns
    -------
    pd.DataFrame
        With columns: hostname, value, timestamp, chart_id, chart_context,
        chart_family, id, name, units (matching legacy CSV schema).
    """
    path = os.path.join(PARQUET_ROOT, kd, f"{test_type}_run{run_num}.parquet")
    if not os.path.exists(path):
        return pd.DataFrame()

    con = duckdb.connect()
    try:
        where_clauses = []
        if contexts:
            ctx_list = ", ".join(f"'{c}'" for c in contexts)
            where_clauses.append(f"chart_context IN ({ctx_list})")
        if hostname:
            where_clauses.append(f"hostname = '{hostname}'")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        sql = f"""
            SELECT
                hostname,
                value,
                relative_time AS timestamp,
                chart_id,
                chart_context,
                chart_family,
                metric_id AS id,
                metric_name AS name,
                units
            FROM '{path}'
            {where_sql}
        """
        df = con.execute(sql).fetchdf()
    finally:
        con.close()

    return df


def load_all_runs(
    kd: str,
    test_type: str,
    contexts: list[str] | None = None,
    hostname: str | None = None,
) -> list[tuple[int, pd.DataFrame]]:
    """
    Load all available runs for a (kd, test_type) pair.

    Returns list of (run_num, DataFrame) tuples for runs that have data.
    """
    results = []
    for run_num in range(1, NUM_RUNS + 1):
        df = load_run(kd, test_type, run_num, contexts=contexts, hostname=hostname)
        if not df.empty:
            results.append((run_num, df))
    return results
