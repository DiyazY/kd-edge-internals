"""
Shared configuration for the CPU microarchitecture and energy analysis.

Self-contained — does not depend on overhead-decomposition at runtime.
"""

import os

# ═══════════════════════════════════════════════════════════════════════════════
#  Paths
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PROJECT_ROOT)

# Shared parquet data (225 files: 5 KDs × 9 test types × 5 runs)
PARQUET_ROOT = os.path.join(REPO_ROOT, "data", "raw")

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "figures")

NUM_RUNS = 5  # Number of experimental runs per test type

# === DEPRECATED — kept so extraction scripts don't crash on import ===
MICROARCH_DATA_DIR = os.path.join(DATA_DIR, "microarch")
MONGO_URI = "mongodb://localhost:27018"
MONGO_DB = "netdata"
MONGO_COLLECTION = "metrics"

# ═══════════════════════════════════════════════════════════════════════════════
#  Kubernetes Distributions
# ═══════════════════════════════════════════════════════════════════════════════

KDS = ["k3s", "k0s", "k8s", "kubeEdge", "openYurt"]
KD_ORDER = ["k0s", "k3s", "k8s", "kubeEdge", "openYurt"]
KD_DISPLAY_NAMES = {
    "k3s": "k3s",
    "k0s": "k0s",
    "k8s": "k8s",
    "kubeEdge": "KubeEdge",
    "openYurt": "OpenYurt",
}

KD_COLORS = {
    "k0s": "#1f77b4",       # blue
    "k3s": "#d62728",       # red
    "k8s": "#2ca02c",       # green
    "kubeEdge": "#9467bd",  # purple
    "openYurt": "#bcbd22",  # yellow-green
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Test Types
# ═══════════════════════════════════════════════════════════════════════════════

TEST_TYPES = ["idle", "cp_heavy_12client", "dp_redis_density"]
EXTENDED_TEST_TYPES = [
    "idle", "cp_light_1client", "cp_heavy_8client",
    "cp_heavy_12client", "dp_redis_density",
]

# ═══════════════════════════════════════════════════════════════════════════════
#  Node Specifications
# ═══════════════════════════════════════════════════════════════════════════════

NODES = {
    "master": {
        "hostname": "master",
        "cpu_cores": 12,  # i7-10710U: 6 cores, 12 threads
        "ram_mb": 65536,
        "type": "NUC",
    },
    "node_1": {
        "hostname": "node_1",
        "cpu_cores": 4,  # Cortex-A72 @ 1.8 GHz
        "ram_mb": 8192,
        "type": "RPi4",
    },
    "node_2": {
        "hostname": "node_2",
        "cpu_cores": 4,
        "ram_mb": 8192,
        "type": "RPi4",
    },
    "node_3": {
        "hostname": "node_3",
        "cpu_cores": 4,
        "ram_mb": 8192,
        "type": "RPi4",
    },
}

NODE_CPU_CAPACITY = {h: n["cpu_cores"] * 100 for h, n in NODES.items()}
WORKER_NODES = ["node_1", "node_2", "node_3"]

# ═══════════════════════════════════════════════════════════════════════════════
#  RPi4 Power Model Specifications (ARM Cortex-A72)
# ═══════════════════════════════════════════════════════════════════════════════

RPI4_SPECS = {
    "soc": "BCM2711",
    "cpu": "Cortex-A72",
    "cores": 4,
    "max_freq_mhz": 1800,
    "min_freq_mhz": 600,
    # Voltage scaling (approximate for ARM Cortex-A72 on BCM2711)
    "voltage_max_v": 1.0,       # At 1.8 GHz
    "voltage_min_v": 0.8,       # At 600 MHz
    # Empirical whole-board power measurements (RPi4 Model B, 8GB)
    # Source: Pidramble USB-C power meter benchmarks [Geerling 2024]
    "power_full_load_w": 6.4,   # All 4 cores at 100% (stress --cpu 4), wall-level
    "power_idle_w": 2.7,        # Bare board idle (no K8s), wall-level
    # Static power = bare-board idle (non-CPU floor: GPU, RAM, USB, Ethernet, VReg)
    "power_static_w": 2.7,
    # Dynamic power budget for 4 CPU cores combined
    # = power_full_load - power_static = 6.4 - 2.7 = 3.7W
    # Per-core dynamic budget at max freq = 3.7 / 4 = 0.925W
    # C_eff = 3.7 / (1.0^2 * 1.8e9 * 4) ≈ 5.14e-10 F
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Target Chart Contexts for Microarchitecture Analysis
# ═══════════════════════════════════════════════════════════════════════════════

MICROARCH_CONTEXTS = [
    "cpufreq.cpufreq",                        # CPU frequency per core (MHz) — universal
    "cpu.core_throttling",                     # Per-core throttling events — universal (master)
]

# System-level aggregate contexts — available for ALL 5 KDs.
# These provide the same data as cpu.* but aggregated across all cores.
# k0s has both cpu.* and system.* variants; other KDs have system.* only.
SYSTEM_INTERRUPT_CONTEXTS = [
    "system.interrupts",                       # Hardware interrupt counts (all KDs)
    "system.softirqs",                         # Software interrupt counts (all KDs)
    "system.softnet_stat",                     # Network soft interrupt stats (all KDs)
]

# ═══════════════════════════════════════════════════════════════════════════════
#  CIS Security Scores (from kube-bench, for cross-referencing)
# ═══════════════════════════════════════════════════════════════════════════════

SECURITY_SCORES = {
    "k3s": 7.21,
    "k0s": 23.69,
    "k8s": 55.0,
    "kubeEdge": 55.0,
    "openYurt": 55.0,
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Publication Constants (from Publications 1 & 2, for cross-referencing)
# ═══════════════════════════════════════════════════════════════════════════════

# System idle CPU usage (% of total) per KD — from Publication 1
SYSTEM_IDLE_CPU_PCT = {
    "k3s": 1.5,
    "k0s": 4.0,
    "k8s": 5.2,
    "kubeEdge": 6.8,
    "openYurt": 5.6,
}

# Data-plane throughput (ops/sec) from memtier benchmark — Publication 1
DP_THROUGHPUT_OPS = {
    "k3s": 17200,
    "k0s": 16800,
    "k8s": 19000,
    "kubeEdge": 4750,
    "openYurt": 18200,
}

# Data-plane average latency (ms) from memtier benchmark — Publication 1
DP_AVG_LATENCY_MS = {
    "k3s": 11.6,
    "k0s": 11.9,
    "k8s": 10.5,
    "kubeEdge": 42.1,
    "openYurt": 11.0,
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Figure Settings
# ═══════════════════════════════════════════════════════════════════════════════

FIGURE_DPI = 300
FIGURE_FORMAT = "pdf"
FIGURE_SIZE = (6, 4.1)
FIGURE_SIZE_WIDE = (8, 4.1)
