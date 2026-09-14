"""Paths and snapshot selection for the analysis layer (the pipeline keeps its own CONFIG)."""

import os
import re
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MART_DIR = PROJECT_ROOT / 'data' / 'mart'
REPORTS_DIR = PROJECT_ROOT / 'reports'
FIGURES_DIR = REPORTS_DIR / 'figures'

# Mainboard and SME are analysed side by side, never pooled
SEGMENTS = ('MAINBOARD', 'SME')

# Set IPO_SNAPSHOT=YYYY-MM-DD to pin a snapshot; otherwise the latest mart export is used
SNAPSHOT_ENV = 'IPO_SNAPSHOT'
_MART_FILE = re.compile(r'^mart_ipo_analytics_(\d{4}-\d{2}-\d{2})\.parquet$')


def available_snapshots(mart_dir: Path = MART_DIR) -> list[date]:
    """Snapshot dates that have a mart Parquet export, oldest first"""
    if not mart_dir.exists():
        return []
    found = (_MART_FILE.match(path.name) for path in mart_dir.iterdir())
    return sorted(date.fromisoformat(match.group(1)) for match in found if match)


def resolve_snapshot(snapshot: date | str | None = None, mart_dir: Path = MART_DIR) -> date:
    """The requested snapshot, else $IPO_SNAPSHOT, else the latest export"""
    snapshot = snapshot or os.environ.get(SNAPSHOT_ENV)
    if snapshot:
        return date.fromisoformat(snapshot) if isinstance(snapshot, str) else snapshot
    snapshots = available_snapshots(mart_dir)
    if not snapshots:
        raise FileNotFoundError(
            f"No mart export in {mart_dir}. Build one with: python pipeline.py --snapshot YYYY-MM-DD --skip-scrape"
        )
    return snapshots[-1]
