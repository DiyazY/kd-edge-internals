# kd-edge-internals

Companion data and analysis pipelines for research on Kubernetes distribution overhead and energy consumption on edge hardware.

This repository contains the raw monitoring data, analysis scripts, and generated results referenced in:

1. **Energy-Aware Kubernetes Distribution Selection for Battery-Constrained Edge Devices** (energy-analysis)
2. **Per-Container Overhead Decomposition of Lightweight Kubernetes on Edge Hardware** (overhead-decomposition)

Both papers build on data collected during the benchmarking campaigns described in:
- [ESOCC 2025 Paper 1: Performance and Resource Efficiency](https://doi.org/10.1007/978-3-031-84617-5_7)
- [ESOCC 2025 Paper 2: Security, Resilience and Maintainability](https://doi.org/10.1007/978-3-031-84617-5_8)

Experimental infrastructure: [github.com/DiyazY/iot-edge](https://github.com/DiyazY/iot-edge)

## Repository Structure

```
kd-edge-internals/
├── data/
│   ├── raw/                        # 225 Parquet files (Git LFS)
│   │   ├── k0s/                    # 9 test types × 5 runs
│   │   ├── k3s/
│   │   ├── k8s/
│   │   ├── kubeEdge/
│   │   └── openYurt/
│   ├── benchmarks/                 # k-bench and system-level CSVs
│   ├── tag_registry.json           # MongoDB tag → test mapping
│   └── probe_report.json           # Data availability matrix
│
├── energy-analysis/                # Energy paper pipeline
│   ├── generate_figures.py         # Single entry point
│   ├── config.py                   # Configuration
│   ├── data_loading.py             # DuckDB parquet loader
│   ├── results/                    # 15 generated CSVs
│   └── figures/                    # 10 generated PDFs
│
├── overhead-decomposition/         # Overhead paper pipeline
│   ├── prepare_silver.py           # Raw → silver transformation
│   ├── config.py
│   ├── data_loading.py
│   ├── classification/             # Pod role classification
│   ├── analysis/                   # Idle/loaded decomposition
│   ├── visualization/              # Publication figures
│   ├── data/silver/                # Pre-aggregated metrics
│   ├── results/                    # 5 generated CSVs
│   └── figures/                    # 5 generated PDFs
│
└── extraction/                     # Reference: how raw data was extracted
    ├── extract_universal.py        # MongoDB → Parquet (requires MongoDB)
    ├── build_registry.py           # Tag registry builder
    └── config.py
```

## Quick Start

### Prerequisites

```bash
pip install -r requirements.txt
```

Requires Python 3.10+. Uses Git LFS for parquet files — ensure `git lfs pull` has been run after cloning.

### Reproduce Energy Analysis Figures

```bash
cd energy-analysis
python generate_figures.py
```

Produces 10 PDF figures in `figures/` and 15 CSV result files in `results/`.

### Reproduce Overhead Decomposition Figures

```bash
cd overhead-decomposition

# Step 1: Transform raw parquets → silver layer
python prepare_silver.py

# Step 2: Classify pods as system vs workload
python -m classification.classify_pods

# Step 3: Run idle/loaded decomposition analysis
python -m analysis.idle_decomposition
python -m analysis.loaded_decomposition

# Step 4: Generate publication figures
python -m visualization.stacked_bars
```

Produces 5 PDF figures in `figures/` and 5 CSV files in `results/`.

## Raw Data Schema

Each parquet file contains Netdata monitoring metrics in long format:

| Column | Description |
|--------|-------------|
| `relative_time` | Seconds from test start (0-based) |
| `hostname` | master, node_1, node_2, node_3 |
| `chart_context` | Metric category (system.cpu, cpufreq.cpufreq, k8s.cgroup.*, ...) |
| `chart_id` | Chart instance with device/container info |
| `chart_family` | Chart grouping (cpu, eth0, mmcblk0, ...) |
| `metric_id` | Metric identifier (user, system, idle, read, write, ...) |
| `metric_name` | Human-readable metric name |
| `value` | Numeric measurement |
| `units` | Measurement unit (percentage, KiB/s, packets/s, ...) |

### Test Types

| Test | File Pattern | Description |
|------|-------------|-------------|
| Idle | `idle_run{1-5}.parquet` | Baseline resource consumption |
| CP Light | `cp_light_1client_run{1-5}.parquet` | 1 client creating/deleting pods |
| CP Heavy 8 | `cp_heavy_8client_run{1-5}.parquet` | 8 concurrent clients |
| CP Heavy 12 | `cp_heavy_12client_run{1-5}.parquet` | 12 concurrent clients |
| DP Redis | `dp_redis_density_run{1-5}.parquet` | Redis pod density + memtier |
| Reliability | `reliability-{control,worker}_run{1-5}.parquet` | Node failure/recovery |
| Long Reliability | `reliability-{control,worker}-no-pressure-long_run{1-5}.parquet` | Extended reliability |

## Experimental Setup

- **Master node:** Intel NUC (i7-10710U, 64 GB DDR4, 1 TB NVMe), Ubuntu 22.04.2
- **Worker nodes:** 3x Raspberry Pi 4 Model B (Cortex-A72 @ 1.8 GHz, 8 GB SDRAM), Ubuntu 22.04.2
- **Monitoring:** Netdata v1.42 → MongoDB 7.0
- **Container runtime:** containerd v1.7.11
- **Test protocol:** Each test repeated 5 times with 30-minute stabilization periods

## Kubernetes Distributions

| Distribution | Type | Key Characteristic |
|---|---|---|
| k0s | Lightweight | Best balance of performance/resources |
| k3s | Lightweight | Lowest latency, easiest setup |
| k8s | Full-featured | Highest throughput, complex setup |
| KubeEdge | Edge extension | Best offline autonomy, worst scalability |
| OpenYurt | Edge extension | Good upstream compatibility |

## Data Provenance

The raw parquet files were extracted from a 1.9-billion-document MongoDB database using `extraction/extract_universal.py`. The extraction scripts are included for reference but require the original MongoDB instance (not included) to run.

## License

Research data and analysis code for academic use. If you use this data, please cite the associated publications.
