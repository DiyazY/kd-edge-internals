"""
Shared matplotlib style settings matching publication conventions.
Reuses iot-edge figure conventions for consistency.
"""

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for PDF generation

import matplotlib.pyplot as plt

# Match existing publication style
FIGURE_DPI = 300
FIGURE_FORMAT = "pdf"
FIGURE_SIZE = (6, 4.1)
FIGURE_SIZE_WIDE = (8, 4.1)

# Component colors for k0s system pods (4 pods on worker node)
# Using distinguishable colors from colorbrewer
POD_COLORS = [
    "#e41a1c",  # red
    "#377eb8",  # blue
    "#4daf4a",  # green
    "#984ea3",  # purple
    "#ff7f00",  # orange
    "#f781bf",  # pink
    "#a65628",  # brown
    "#999999",  # gray
]

# Role colors
ROLE_COLORS = {
    "system": "#e41a1c",    # red
    "workload": "#377eb8",  # blue
}

# Test type display names
TEST_TYPE_LABELS = {
    "idle": "Idle",
    "cp_heavy_12client": "CP Heavy\n(12 clients)",
    "dp_redis_density": "DP Redis\nDensity",
}

# QoS class display
QOS_LABELS = {
    "burstable": "Burstable",
    "besteffort": "BestEffort",
}


def setup_style():
    """Configure matplotlib for publication-quality figures."""
    plt.rcParams.update({
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.labelsize": 12,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 9,
        "figure.dpi": FIGURE_DPI,
        "savefig.dpi": FIGURE_DPI,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.1,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "axes.spines.top": False,
        "axes.spines.right": False,
    })


def save_figure(fig, name: str, figures_dir: str):
    """Save a figure as PDF."""
    import os
    os.makedirs(figures_dir, exist_ok=True)
    path = os.path.join(figures_dir, f"{name}.{FIGURE_FORMAT}")
    fig.savefig(path, format=FIGURE_FORMAT)
    plt.close(fig)
    print(f"  Saved: {path}")
    return path
