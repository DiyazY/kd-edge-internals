#!/usr/bin/env python3
"""
Extract all metrics from MongoDB for aligned cross-KD analysis.

For each KD, extracts run 1 of each test type into parquet files with relative
timestamps. Extracts everything within the test's time window — no context
filtering. Covers both stress progression tests (idle → CP → DP) and
reliability tests (control/worker node failure/recovery).

The output is one parquet per (KD, test_type) in long format:
    data/raw/{kd}/{test_type}.parquet

Columns:
    relative_time  - seconds from test start (0-based)
    hostname       - master, node_1, node_2, node_3
    chart_context  - e.g., system.cpu, disk.io, ip.tcpsock
    chart_id       - e.g., system.cpu, disk_mmcblk0.io
    chart_family   - e.g., cpu, mmcblk0, eth0
    metric_id      - e.g., user, system, idle (the 'id' field)
    metric_name    - e.g., user, system, idle (the 'name' field)
    value          - numeric measurement
    units          - e.g., percentage, KiB/s, packets/s

Usage:
    # Extract everything (all 5 KDs, all 9 test types, run 1)
    python extract_universal.py

    # Extract all 5 runs per test (for cross-run averaging)
    python extract_universal.py --all-runs

    # Extract all runs for one KD only
    python extract_universal.py --all-runs --kd k0s

    # Extract specific test type
    python extract_universal.py --kd k8s --test idle

    # Extract only stress or reliability tests
    python extract_universal.py --test reliability-control

    # Use a different run number
    python extract_universal.py --run 2

    # Re-extract (overwrite existing)
    python extract_universal.py --no-skip
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    DATA_DIR,
    DEFAULT_RUN,
    EXTRACT_FIELDS,
    KDS,
    MAX_RUNS,
    MONGO_COLLECTION,
    MONGO_DB,
    MONGO_URI,
    RAW_DATA_DIR,
    TEST_SEQUENCE,
)

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pymongo not installed. Run: pip install pymongo>=4.6")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas>=2.0 pyarrow>=14.0")
    sys.exit(1)


def resolve_mongo_tag(
    collection, tag: str, kd: str, start_ts: int, end_ts: int
) -> str | None:
    """
    Find the actual MongoDB tag, handling k0s trailing periods and prefix variants.
    """
    candidates = [tag]

    # k0s trailing period
    if not tag.endswith("."):
        candidates.append(tag + ".")

    # With KD prefix
    if not tag.startswith(f"{kd}-"):
        candidates.append(f"{kd}-{tag}")
        candidates.append(f"{kd}-{tag}.")

    # Without KD prefix
    if tag.startswith(f"{kd}-"):
        unprefixed = tag[len(f"{kd}-"):]
        candidates.append(unprefixed)

    # Compound tags (comma → period)
    if "," in tag:
        dotted = tag.replace(",", ".") + "."
        candidates.append(dotted)
        candidates.append(tag.replace(",", "."))

    for candidate in candidates:
        query = {
            "labels.tag": candidate,
            "timestamp": {"$gte": start_ts, "$lte": end_ts},
        }
        if collection.count_documents(query, limit=1) > 0:
            return candidate

    return None


def extract_test(
    collection,
    mongo_tag: str,
    start_ts: int,
    end_ts: int,
) -> pd.DataFrame:
    """
    Extract all metrics for a single test run from MongoDB.

    Pulls everything within the tag's time window — no context filtering.
    Returns a DataFrame in long format with relative_time.
    """
    query = {
        "labels.tag": mongo_tag,
        "timestamp": {"$gte": start_ts, "$lte": end_ts},
    }

    projection = {field: 1 for field in EXTRACT_FIELDS}
    projection["_id"] = 0

    cursor = collection.find(query, projection).batch_size(10000)
    records = list(cursor)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Compute relative time (0-based seconds from test start)
    df["relative_time"] = df["timestamp"] - start_ts

    # Rename for clarity
    df = df.rename(columns={"id": "metric_id", "name": "metric_name"})

    # Drop absolute timestamp (we keep relative_time)
    df = df.drop(columns=["timestamp"])

    # Sort for reproducibility
    df = df.sort_values(
        ["relative_time", "hostname", "chart_context", "metric_id"]
    ).reset_index(drop=True)

    return df


def load_registry() -> list[dict]:
    """Load the tag registry JSON."""
    path = os.path.join(DATA_DIR, "tag_registry.json")
    if not os.path.exists(path):
        print(f"ERROR: Registry not found at {path}")
        print("Run: python extraction/build_registry.py")
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def find_run(
    registry: list[dict], kd: str, test_type: str, run_num: int
) -> dict | None:
    """Find a specific run in the registry."""
    for entry in registry:
        if (
            entry["kd"] == kd
            and entry["test_type"] == test_type
            and entry["run_num"] == run_num
            and entry["start_ts"] is not None
        ):
            return entry
    return None


def extract_all(
    kds: list[str] | None = None,
    test_types: list[str] | None = None,
    run_num: int = DEFAULT_RUN,
    skip_existing: bool = True,
    all_runs: bool = False,
):
    """
    Main extraction loop.

    For each (KD, test_type), extracts run(s) from MongoDB and saves as parquet.
    When all_runs=True, extracts runs 1-5 as {test_type}_run{N}.parquet.
    When all_runs=False, extracts single run as {test_type}.parquet (legacy).
    """
    if kds is None:
        kds = KDS
    if test_types is None:
        test_types = TEST_SEQUENCE

    registry = load_registry()

    # Connect to MongoDB
    print(f"Connecting to MongoDB at {MONGO_URI}...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as e:
        print(f"ERROR: Cannot connect to MongoDB: {e}")
        print("Start MongoDB: docker start mongo-backup")
        sys.exit(1)

    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    print(f"Connected to {MONGO_DB}.{MONGO_COLLECTION}")

    # Track results
    results = []
    total_rows = 0
    total_bytes = 0

    if all_runs:
        print(f"\nExtracting runs 1-{MAX_RUNS} for {len(kds)} KDs × {len(test_types)} tests")
        print(f"Output: {RAW_DATA_DIR}/{{kd}}/{{test_type}}_run{{N}}.parquet")
    else:
        print(f"\nExtracting run {run_num} for {len(kds)} KDs × {len(test_types)} tests")
        print(f"Output: {RAW_DATA_DIR}/{{kd}}/{{test_type}}.parquet")
    print("=" * 70)

    for kd in kds:
        kd_dir = os.path.join(RAW_DATA_DIR, kd)
        os.makedirs(kd_dir, exist_ok=True)

        for test_type in test_types:
            runs_to_extract = range(1, MAX_RUNS + 1) if all_runs else [run_num]

            for current_run in runs_to_extract:
                if all_runs:
                    out_path = os.path.join(kd_dir, f"{test_type}_run{current_run}.parquet")
                else:
                    out_path = os.path.join(kd_dir, f"{test_type}.parquet")

                label = f"{kd:12s} {test_type:25s} run{current_run}"

                # Skip if exists
                if skip_existing and os.path.exists(out_path):
                    size = os.path.getsize(out_path)
                    print(f"  {label} → SKIP (exists, {size / 1024:.0f} KB)")
                    results.append({"kd": kd, "test": test_type, "run": current_run, "status": "skipped"})
                    continue

                # Find the run in registry
                entry = find_run(registry, kd, test_type, current_run)
                if entry is None:
                    if not all_runs:
                        print(f"  {label} → NOT FOUND in registry")
                    results.append({"kd": kd, "test": test_type, "run": current_run, "status": "not_found"})
                    continue

                tag = entry["tag"]
                start_ts = entry["start_ts"]
                end_ts = entry["end_ts"]
                duration = entry["duration_s"]

                # Resolve MongoDB tag
                t0 = time.time()
                mongo_tag = resolve_mongo_tag(collection, tag, kd, start_ts, end_ts)
                if mongo_tag is None:
                    print(f"  {label} → TAG NOT RESOLVED: {tag}")
                    results.append({"kd": kd, "test": test_type, "run": current_run, "status": "tag_failed"})
                    continue

                tag_info = f" (→{mongo_tag})" if mongo_tag != tag else ""

                # Extract from MongoDB
                df = extract_test(collection, mongo_tag, start_ts, end_ts)
                elapsed = time.time() - t0

                if df.empty:
                    print(f"  {label} → EMPTY (0 rows, {elapsed:.1f}s){tag_info}")
                    results.append({"kd": kd, "test": test_type, "run": current_run, "status": "empty"})
                    continue

                # Save as parquet
                df.to_parquet(out_path, index=False, engine="pyarrow")
                file_size = os.path.getsize(out_path)
                total_rows += len(df)
                total_bytes += file_size

                # Summary stats
                n_contexts = df["chart_context"].nunique()
                n_metrics = df.groupby(["chart_context", "metric_id"]).ngroups
                n_hosts = df["hostname"].nunique()
                time_range = df["relative_time"].max()

                print(
                    f"  {label} → {len(df):>7,} rows, "
                    f"{n_contexts} contexts, {n_metrics} metrics, "
                    f"{n_hosts} nodes, {time_range}s, "
                    f"{file_size / 1024:.0f} KB "
                    f"({elapsed:.1f}s){tag_info}"
                )

                results.append({
                    "kd": kd,
                    "test": test_type,
                    "run": current_run,
                    "status": "ok",
                    "rows": len(df),
                    "contexts": n_contexts,
                    "metrics": n_metrics,
                    "nodes": n_hosts,
                    "duration_s": duration,
                    "file_kb": round(file_size / 1024),
                })

    # Summary
    ok = sum(1 for r in results if r.get("status") == "ok")
    skipped = sum(1 for r in results if r.get("status") == "skipped")
    failed = sum(1 for r in results if r.get("status") not in ("ok", "skipped"))

    print(f"\n{'=' * 70}")
    print(f"Extraction complete")
    print(f"  Extracted: {ok}, Skipped: {skipped}, Failed: {failed}")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Total size: {total_bytes / (1024 * 1024):.1f} MB")

    # Save manifest
    manifest_path = os.path.join(DATA_DIR, "extraction_manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  Manifest: {manifest_path}")

    client.close()
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract system-level metrics from MongoDB for cross-KD analysis"
    )
    parser.add_argument("--kd", help="Extract only this KD (e.g., k0s)")
    parser.add_argument("--test", help="Extract only this test type (e.g., idle)")
    parser.add_argument("--run", type=int, default=DEFAULT_RUN, help="Run number (default: 1)")
    parser.add_argument("--all-runs", action="store_true",
                        help="Extract all 5 runs per test (output: {test}_run{N}.parquet)")
    parser.add_argument("--no-skip", action="store_true", help="Re-extract existing files")
    args = parser.parse_args()

    kds = [args.kd] if args.kd else None
    tests = [args.test] if args.test else None

    extract_all(
        kds=kds,
        test_types=tests,
        run_num=args.run,
        skip_existing=not args.no_skip,
        all_runs=args.all_runs,
    )
