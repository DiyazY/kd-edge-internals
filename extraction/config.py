"""
Configuration for the multidimensional cross-KD analysis.

Extracts ALL available system-level metrics from MongoDB for all 5 Kubernetes
distributions across all test types. The goal is aligned, comparable datasets
where row N in one KD's data corresponds to the same test phase as row N in another.
"""

import os

# ═══════════════════════════════════════════════════════════════════════════════
#  Paths
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MEGA_RESEARCH_ROOT = os.path.dirname(PROJECT_ROOT)
IOT_EDGE_ROOT = os.path.join(MEGA_RESEARCH_ROOT, "iot-edge")
KBENCH_RESULTS = os.path.join(IOT_EDGE_ROOT, "src", "k-bench-results")

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "figures")

# ═══════════════════════════════════════════════════════════════════════════════
#  MongoDB
# ═══════════════════════════════════════════════════════════════════════════════

MONGO_URI = "mongodb://localhost:27018"
MONGO_DB = "netdata"
MONGO_COLLECTION = "metrics"

# ═══════════════════════════════════════════════════════════════════════════════
#  Kubernetes Distributions
# ═══════════════════════════════════════════════════════════════════════════════

KDS = ["k0s", "k3s", "k8s", "kubeEdge", "openYurt"]
KD_DISPLAY = {
    "k0s": "k0s", "k3s": "k3s", "k8s": "k8s",
    "kubeEdge": "KubeEdge", "openYurt": "OpenYurt",
}
KD_COLORS = {
    "k0s": "#1f77b4", "k3s": "#d62728", "k8s": "#2ca02c",
    "kubeEdge": "#9467bd", "openYurt": "#bcbd22",
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Test Types
# ═══════════════════════════════════════════════════════════════════════════════

# Stress progression narrative: idle → light CP → medium CP → heavy CP → DP
STRESS_TESTS = [
    "idle",
    "cp_light_1client",
    "cp_heavy_8client",
    "cp_heavy_12client",
    "dp_redis_density",
]

# Reliability tests: node failure/recovery scenarios
RELIABILITY_TESTS = [
    "reliability-control",
    "reliability-worker",
    "reliability-control-no-pressure-long",
    "reliability-worker-no-pressure-long",
]

# Full sequence: stress progression followed by reliability scenarios
TEST_SEQUENCE = STRESS_TESTS + RELIABILITY_TESTS

# Which run to extract (run 1 is the baseline; all KDs have at least run 1)
DEFAULT_RUN = 1

# Maximum number of runs available per test type
MAX_RUNS = 5

# ═══════════════════════════════════════════════════════════════════════════════
#  Nodes
# ═══════════════════════════════════════════════════════════════════════════════

HOSTNAMES = ["master", "node_1", "node_2", "node_3"]
WORKER_NODES = ["node_1", "node_2", "node_3"]

NODES = {
    "master": {"cpu_cores": 12, "ram_mb": 65536, "type": "NUC"},
    "node_1": {"cpu_cores": 4, "ram_mb": 8192, "type": "RPi4"},
    "node_2": {"cpu_cores": 4, "ram_mb": 8192, "type": "RPi4"},
    "node_3": {"cpu_cores": 4, "ram_mb": 8192, "type": "RPi4"},
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Context Extraction Pattern
# ═══════════════════════════════════════════════════════════════════════════════

# Regex matching all system-level chart contexts (both universal and per-core).
# This intentionally captures k0s-only per-core contexts (cpu.interrupts, etc.)
# alongside universal ones — non-k0s KDs simply won't have those rows.
#
# Excludes: k8s.cgroup.* (per-container), netdata.* (monitoring internals),
#           services.* (systemd, k0s-only), exporting_* (Netdata export)
CONTEXT_PATTERN = (
    r"^("
    r"system\.|"          # 38 universal: cpu, ram, io, load, pressure, etc.
    r"disk\.|"            # 12 universal: io, ops, busy, util, await, etc.
    r"disk_ext\.|"        #  7 universal: extended disk metrics
    r"net\.|"             #  8 universal: interface traffic, packets, drops
    r"ip\.|"              #  9 universal: TCP lifecycle (opens, errors, reorders)
    r"ipv4\.|"            # 12 universal: IPv4 packets, sockets, UDP
    r"ipv6\.|"            # 13 universal: IPv6 packets, ICMPv6
    r"mem\.|"             # 11 universal: available, kernel, slab, pgfaults, oom
    r"cpufreq\.|"         #  1 universal: CPU frequency per core
    r"cpu\.|"             #  2 universal (core_throttling) + k0s-only per-core
    r"cpuidle\.|"         #  k0s-only: C-state residency
    r"pci\.|"             #  2 universal: PCIe AER errors
    r"netfilter\."        #  1 universal: conntrack sockets
    r")"
)

# Fields to extract from MongoDB documents
EXTRACT_FIELDS = [
    "hostname",
    "timestamp",
    "chart_context",
    "chart_id",
    "chart_family",
    "id",
    "name",
    "value",
    "units",
]
