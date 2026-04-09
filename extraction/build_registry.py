#!/usr/bin/env python3
"""
Build the full tag registry for all 5 KDs across all test types.

Walks iot-edge/src/k-bench-results/{kd}/{test_type}/{run}/ and extracts:
- Tag name (from ansible_output_*.txt)
- Start/end timestamps (from tmp-before.txt/tmp-after.txt or ansible msg)
- KD, test type, run directory name

Output: data/tag_registry.json
"""

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KBENCH_RESULTS, KDS, TEST_SEQUENCE, DATA_DIR


def parse_tag_from_ansible(filepath: str) -> str | None:
    """Extract tag name from ansible output file."""
    try:
        with open(filepath) as f:
            content = f.read()
        match = re.search(r"change the tag to ([^\s\]]+)", content)
        if match:
            return match.group(1).rstrip("*").rstrip(",")
    except (FileNotFoundError, IOError):
        pass
    return None


def parse_timestamps_from_files(run_dir: str) -> tuple[int | None, int | None]:
    """Extract start/end from tmp-before.txt / tmp-after.txt."""
    start, end = None, None
    for fname, target in [("tmp-before.txt", "start"), ("tmp-after.txt", "end")]:
        try:
            with open(os.path.join(run_dir, fname)) as f:
                ts = int(f.read().strip())
                if target == "start":
                    start = ts
                else:
                    end = ts
        except (FileNotFoundError, ValueError):
            pass
    return start, end


def parse_timestamps_from_ansible(filepath: str) -> tuple[int | None, int | None]:
    """Extract timestamps from ansible output 'msg' line (k0s pattern)."""
    try:
        with open(filepath) as f:
            content = f.read()
        match = re.search(r'"msg":\s*"(\d+)\s*-\s*(\d+)"', content)
        if match:
            return int(match.group(1)), int(match.group(2))
    except (FileNotFoundError, IOError):
        pass
    return None, None


def extract_run_number(run_dir_name: str) -> int | None:
    """Extract numeric run index from directory name like 'idle-1' or 'cp_light_1client-3'."""
    match = re.search(r"-(\d+)$", run_dir_name)
    return int(match.group(1)) if match else None


def build_registry() -> list[dict]:
    """Build the full tag registry from k-bench results."""
    registry = []

    for kd in KDS:
        kd_path = os.path.join(KBENCH_RESULTS, kd)
        if not os.path.isdir(kd_path):
            print(f"  [WARN] KD dir not found: {kd_path}")
            continue

        for test_type in TEST_SEQUENCE:
            test_path = os.path.join(kd_path, test_type)
            if not os.path.isdir(test_path):
                print(f"  [WARN] Test dir not found: {kd}/{test_type}")
                continue

            for run_dir_name in sorted(os.listdir(test_path)):
                run_path = os.path.join(test_path, run_dir_name)
                if not os.path.isdir(run_path):
                    continue

                # Parse tag from ansible output
                tag = None
                ansible_start, ansible_end = None, None
                for fname in os.listdir(run_path):
                    if fname.startswith("ansible_output_"):
                        fpath = os.path.join(run_path, fname)
                        tag = tag or parse_tag_from_ansible(fpath)
                        a_start, a_end = parse_timestamps_from_ansible(fpath)
                        if a_start is not None:
                            ansible_start, ansible_end = a_start, a_end

                # Prefer tmp-before/after (more reliable), fallback to ansible msg
                file_start, file_end = parse_timestamps_from_files(run_path)
                start_ts = file_start or ansible_start
                end_ts = file_end or ansible_end

                if tag is None:
                    tag = run_dir_name

                run_num = extract_run_number(run_dir_name)

                entry = {
                    "kd": kd,
                    "test_type": test_type,
                    "run_dir": run_dir_name,
                    "run_num": run_num,
                    "tag": tag,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "duration_s": (end_ts - start_ts) if start_ts and end_ts else None,
                }
                registry.append(entry)

    return registry


def print_summary(registry: list[dict]):
    """Print a coverage table."""
    print(f"\n{'Tag Registry Summary':^70}")
    print("=" * 70)

    for kd in KDS:
        entries = [e for e in registry if e["kd"] == kd]
        print(f"\n  {kd} ({len(entries)} entries):")

        for test_type in TEST_SEQUENCE:
            runs = [e for e in entries if e["test_type"] == test_type]
            has_ts = sum(1 for r in runs if r["start_ts"] is not None)
            durations = [r["duration_s"] for r in runs if r["duration_s"]]
            dur_range = f"{min(durations)}-{max(durations)}s" if durations else "N/A"
            print(
                f"    {test_type:30s} {len(runs):2d} runs, "
                f"{has_ts} with timestamps, dur={dur_range}"
            )

    # Check for issues
    missing_ts = [e for e in registry if e["start_ts"] is None]
    if missing_ts:
        print(f"\n  [WARN] {len(missing_ts)} entries missing timestamps")


if __name__ == "__main__":
    print("Building full tag registry...")
    print(f"  KDs: {KDS}")
    print(f"  Test types: {TEST_SEQUENCE}")

    registry = build_registry()

    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, "tag_registry.json")
    with open(out_path, "w") as f:
        json.dump(registry, f, indent=2)

    print_summary(registry)
    print(f"\nSaved {len(registry)} entries to: {out_path}")
