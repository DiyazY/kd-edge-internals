"""
Shared constants for the per-container overhead decomposition analysis.
"""
import os

# === Paths ===
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(PROJECT_ROOT)

# Shared parquet data (225 files: 5 KDs × 9 test types × 5 runs)
PARQUET_ROOT = os.path.join(REPO_ROOT, "data", "raw")

# Benchmark data from iot-edge and nuc (for proposition testing)
BENCHMARKS_DIR = os.path.join(REPO_ROOT, "data", "benchmarks")
KBENCH_RESULTS = BENCHMARKS_DIR
NUC_TEST_RESULTS = os.path.join(BENCHMARKS_DIR, "system-level")

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "figures")

# === Kubernetes Distributions ===
KDS = ["k3s", "k0s", "k8s", "kubeEdge", "openYurt"]
KD_DISPLAY_NAMES = {
    "k3s": "k3s",
    "k0s": "k0s",
    "k8s": "k8s",
    "kubeEdge": "KubeEdge",
    "openYurt": "OpenYurt",
}

# === Test Types (focus for overhead decomposition) ===
TEST_TYPES = ["idle", "cp_heavy_12client", "dp_redis_density"]
NUM_RUNS = 5  # Number of experimental runs for cross-run averaging

# === Node Specifications ===
NODES = {
    "master": {
        "hostname": "master",
        "cpu_cores": 12,  # i7-10710U: 6 cores, 12 threads
        "ram_mb": 65536,  # 64 GB DDR4
        "type": "NUC",
    },
    "node_1": {
        "hostname": "node_1",
        "cpu_cores": 4,  # Cortex-A72 @ 1.8 GHz
        "ram_mb": 8192,  # 8 GB SDRAM
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

# Total CPU capacity in percentage points (cores * 100%)
NODE_CPU_CAPACITY = {h: n["cpu_cores"] * 100 for h, n in NODES.items()}
NODE_RAM_MB = {h: n["ram_mb"] for h, n in NODES.items()}

# === Visualization (matching publication style from box-plotting.py) ===
KD_COLORS = {
    "k0s": "#1f77b4",       # blue
    "k3s": "#d62728",       # red
    "k8s": "#2ca02c",       # green
    "kubeEdge": "#9467bd",  # purple
    "openYurt": "#bcbd22",  # yellow-green
}
KD_ORDER = ["k0s", "k3s", "k8s", "kubeEdge", "openYurt"]

COMPONENT_COLORS = {
    "etcd": "#e41a1c",
    "kube-apiserver": "#377eb8",
    "kube-controller-manager": "#4daf4a",
    "kube-scheduler": "#984ea3",
    "kube-proxy": "#ff7f00",
    "coredns": "#a65628",
    "flannel": "#f781bf",
    "cloudcore": "#999999",
    "edgemesh": "#66c2a5",
    "yurt-hub": "#fc8d62",
    "yurt-controller": "#8da0cb",
    "yurt-tunnel": "#e78ac3",
    "local-path-provisioner": "#a6d854",
    "pause": "#cccccc",
    "other-system": "#e5c494",
    "workload": "#66c2a5",
}

FIGURE_DPI = 300
FIGURE_FORMAT = "pdf"
FIGURE_SIZE = (6, 4.1)  # Matching existing publications

# === Pod Classification Keywords ===
SYSTEM_KEYWORDS = [
    "kube-proxy", "coredns", "kube-dns", "etcd",
    "flannel", "calico", "kube-router", "kindnet",
    "kube-apiserver", "kube-controller", "kube-scheduler",
    "cloudcore", "edgecore", "edgemesh",
    "yurt-hub", "yurt-controller", "yurt-tunnel", "yurt-manager",
    "pause", "metrics-server", "local-path-provisioner",
]

WORKLOAD_KEYWORDS = [
    "redis", "kbench", "k-bench", "nginx", "memtier",
    "test-pod", "bench-", "density",
]

# === CIS Security Scores (from kube-bench final reports) ===
SECURITY_SCORES = {
    "k3s": 7.21,
    "k0s": 23.69,
    "k8s": 55.0,
    "kubeEdge": 55.0,
    "openYurt": 55.0,
}
