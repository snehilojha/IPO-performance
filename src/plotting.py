"""One chart style for notebooks and reports, and a single place figures are saved."""

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

from src.config import FIGURES_DIR

SEGMENT_COLORS = {'MAINBOARD': '#1f5aa6', 'SME': '#d9730d'}
SEGMENT_LABELS = {'MAINBOARD': 'Mainboard', 'SME': 'SME'}
MUTED = '#6b7280'

# Linear within +/- this many percent, logarithmic beyond: returns range from -100% to several thousand
ASINH_LINEAR_WIDTH = 10

STYLE = {
    'figure.figsize': (10, 4.5),
    'figure.dpi': 110,
    'savefig.dpi': 150,
    'savefig.bbox': 'tight',
    'font.size': 10,
    'axes.titlesize': 11,
    'axes.titleweight': 'bold',
    'axes.titlelocation': 'left',
    'axes.labelsize': 10,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.grid': True,
    'grid.color': '#e5e7eb',
    'grid.linewidth': 0.8,
    'axes.axisbelow': True,
    'legend.frameon': False,
}


def apply_style():
    mpl.rcParams.update(STYLE)


def save_fig(fig: plt.Figure, name: str, figures_dir: Path = FIGURES_DIR) -> Path:
    """Write fig to <figures_dir>/<name>.png, overwriting the previous version"""
    figures_dir.mkdir(parents=True, exist_ok=True)
    path = figures_dir / f'{name}.png'
    fig.savefig(path)
    return path


def asinh_bins(values, n_bins: int = 40, linear_width: float = ASINH_LINEAR_WIDTH) -> np.ndarray:
    """Histogram edges evenly spaced on an asinh axis, so bars look equal width on that scale"""
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    low, high = np.arcsinh(np.array([values.min(), values.max()]) / linear_width)
    return linear_width * np.sinh(np.linspace(low, high, n_bins + 1))


def percent_axis(ax: plt.Axes, axis: str = 'x'):
    """Tick labels as +12% / -5%"""
    formatter = mpl.ticker.FuncFormatter(lambda v, _: f'{v:+.0f}%' if v else '0%')
    (ax.xaxis if axis == 'x' else ax.yaxis).set_major_formatter(formatter)
