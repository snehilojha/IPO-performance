"""Cached data loading and formatting shared by the dashboard pages."""

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
import streamlit as st

from src.config import SEGMENTS, resolve_snapshot
from src.dashboard import with_derived_columns
from src.data import load_funnel, load_mart
from src.plotting import SEGMENT_COLORS, SEGMENT_LABELS

REPO_URL = 'https://github.com/snehilojha/IPO-performance'
PLOTLY_CONFIG = {'displaylogo': False}


@dataclass(frozen=True)
class DashboardData:
    snapshot: date
    mart: pd.DataFrame
    funnel: pd.DataFrame


@st.cache_data(show_spinner='Loading the mart')
def _load(snapshot: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    return with_derived_columns(load_mart(snapshot)), load_funnel(snapshot)


def load_data() -> DashboardData:
    """The latest mart export (or $IPO_SNAPSHOT) with the dashboard's derived columns, cached across reruns"""
    snapshot = resolve_snapshot()
    mart, funnel = _load(snapshot)
    return DashboardData(snapshot, mart, funnel)


def segment_label(segment: str) -> str:
    return SEGMENT_LABELS[segment]


def segment_colors() -> dict[str, str]:
    """Plotly colour map keyed by the display label, which is what the charts use"""
    return {SEGMENT_LABELS[s]: SEGMENT_COLORS[s] for s in SEGMENTS}


def with_segment_label(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Segment=df['segment'].astype(str).map(SEGMENT_LABELS))


def pct(value: float, digits: int = 1, signed: bool = True) -> str:
    """12.3 -> '+12.3%'; NaN -> '–'"""
    if value is None or pd.isna(value):
        return '–'
    return f'{value:+.{digits}f}%' if signed else f'{value:.{digits}f}%'


def pp(value: float, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return '–'
    return f'{value:+.{digits}f} pp'


def share(value: float) -> str:
    """0.713 -> '71%'"""
    if value is None or pd.isna(value):
        return '–'
    return f'{100 * value:.0f}%'


def times(value: float) -> str:
    """Subscription multiple: 0.53 -> '0.53×', 38.25 -> '38.3×', 1234 -> '1,234×'"""
    if value is None or pd.isna(value):
        return '–'
    if value < 10:
        return f'{value:.2f}×'
    return f'{value:,.1f}×' if value < 1000 else f'{value:,.0f}×'


def rupees(value: float, unit: str = '') -> str:
    if value is None or pd.isna(value):
        return '–'
    return f'₹{value:,.2f}{unit}' if value < 1000 else f'₹{value:,.0f}{unit}'


def log_ticks(low: float, high: float, max_ticks: int = 8) -> list[float]:
    """
    Tick values covering [low, high] for log axes that should read 1×, 10×, 100× rather than 10^x:
    1-2-5 steps for a narrow range, then 1-3, then powers of ten, whichever first fits in max_ticks
    """
    for steps in ((1, 2, 5), (1, 3), (1,)):
        ticks = [m * 10.0 ** e for e in range(-2, 5) for m in steps]
        ticks = [t for t in ticks if low / 1.5 <= t <= high * 1.5]
        if len(ticks) <= max_ticks:
            break
    return ticks or [float(np.round(low, 2))]


def tick_labels(ticks: list[float]) -> list[str]:
    return [f'{t:,g}×' for t in ticks]
