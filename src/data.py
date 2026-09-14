"""Load the mart and join funnel exports with analysis-friendly dtypes."""

from datetime import date
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import MART_DIR, REPORTS_DIR, SEGMENTS, resolve_snapshot

HORIZONS = ('1m', '6m', '1y', '3y')
HORIZON_MONTHS = {'1m': 1, '6m': 6, '1y': 12, '3y': 36}

# SME listing gains can't exceed these: NSE's +90% pre-open cap from 2024-07-04 (documented), and an observed
# +20% ceiling until the first SME listing above it on 2018-04-26 (no circular found).
SME_CAP_START = pd.Timestamp('2024-07-04')
SME_EARLY_CEILING_END = pd.Timestamp('2018-04-26')


def load_mart(snapshot: date | str | None = None, mart_dir: Path = MART_DIR) -> pd.DataFrame:
    """
    mart_ipo_analytics for one snapshot, one row per listed IPO.

    DuckDB writes DATE as datetime.date, DECIMAL as Decimal and nullable BOOLEAN as object columns;
    these become datetime64, float and pandas' nullable boolean so NULL stays NULL. Adds listing_year.
    """
    snapshot = resolve_snapshot(snapshot, mart_dir)
    path = mart_dir / f'mart_ipo_analytics_{snapshot.isoformat()}.parquet'
    if not path.exists():
        raise FileNotFoundError(f"No mart export for snapshot {snapshot}: {path}")
    df = pd.read_parquet(path)

    for column in df.columns:
        values = df[column].dropna()
        if values.empty or df[column].dtype != object:
            continue
        first = values.iloc[0]
        if isinstance(first, bool):
            df[column] = df[column].astype('boolean')
        elif isinstance(first, date):
            df[column] = pd.to_datetime(df[column])
        elif isinstance(first, Decimal):
            df[column] = pd.to_numeric(df[column].map(lambda v: None if v is None else float(v)))

    df['segment'] = pd.Categorical(df['segment'], categories=SEGMENTS, ordered=True)
    df['listing_year'] = df['listing_dt'].dt.year.astype('Int64')
    return df


def load_funnel(snapshot: date | str | None = None, reports_dir: Path = REPORTS_DIR,
                mart_dir: Path = MART_DIR) -> pd.DataFrame:
    """audit_join_funnel for one snapshot as a step x segment table of row counts"""
    snapshot = resolve_snapshot(snapshot, mart_dir)
    funnel = pd.read_csv(reports_dir / f'join_funnel_{snapshot.isoformat()}.csv')
    table = funnel.pivot_table(index=['step', 'stage', 'description'], columns='segment', values='n',
                               aggfunc='sum')
    # Step 1 (the raw feed) has no segment split, so counts are nullable integers
    return table[['ALL', *SEGMENTS]].astype('Int64').reset_index().rename_axis(columns=None)


def listing_ceiling_pct(df: pd.DataFrame) -> pd.Series:
    """The highest listing gain allowed on the listing date (90 or 20 for SME in the capped periods), else NaN"""
    sme = df['segment'] == 'SME'
    ceiling = pd.Series(np.nan, index=df.index)
    ceiling[sme & (df['listing_dt'] >= SME_CAP_START)] = 90.0
    ceiling[sme & (df['listing_dt'] < SME_EARLY_CEILING_END)] = 20.0
    return ceiling


def at_listing_ceiling(df: pd.DataFrame) -> pd.Series:
    """
    True where the listing-day open sits at its ceiling, so the listing gain is a lower bound on demand.
    Same rule as the SQL listing_capped: open within one 0.05 price tick of the rounded ceiling price.
    """
    ceiling_price = (df['issue_price'] * (1 + listing_ceiling_pct(df) / 100)).round(2)
    # SQL compares exact DECIMALs; the 1e-6 absorbs float error such as 68.40 - 0.05 = 68.35000000000001
    return (df['listing_open'] >= ceiling_price - 0.05 - 1e-6).fillna(False).astype(bool)


def horizon_elapsed(df: pd.DataFrame, horizon: str) -> pd.Series:
    """True where listing_dt + horizon is on or before the snapshot date, so a return could exist"""
    return df['listing_dt'] + pd.DateOffset(months=HORIZON_MONTHS[horizon]) <= df['snapshot_date']
