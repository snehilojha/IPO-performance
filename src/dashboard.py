"""Tables behind the Streamlit dashboard, kept free of Streamlit so they can be tested."""

import numpy as np
import pandas as pd

from src.config import SEGMENTS
from src.data import HORIZONS, at_listing_ceiling, horizon_elapsed
from src.stats import decile_table

# Chittorgarh has no SME subscription figures for 2016 and few for 2017; the notebooks start SME in 2018
SME_SUBSCRIPTION_START = pd.Timestamp('2018-01-01')
NO_SECTOR = 'No sector (no Tickertape match)'
SUBSCRIPTION_CATEGORIES = {
    'subscription_qib_x': 'QIB',
    'subscription_nii_x': 'NII',
    'subscription_retail_x': 'Retail',
    'subscription_total_x': 'Total',
}


def with_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """The mart plus the columns every page uses: sector with a label for missing, and the listing-ceiling flag"""
    df = df.copy()
    df['sector_label'] = df['sector'].fillna(NO_SECTOR)
    df['at_listing_ceiling'] = at_listing_ceiling(df)
    df['in_subscription_sample'] = subscription_sample(df)
    return df


def subscription_sample(df: pd.DataFrame) -> pd.Series:
    """The notebooks' sample for subscription vs listing gain: both measured, SME from 2018"""
    return (df['has_listing_price'] & df['has_subscription_figures']
            & ((df['segment'] == 'MAINBOARD') | (df['listing_dt'] >= SME_SUBSCRIPTION_START)))


def filter_ipos(df: pd.DataFrame, segments=SEGMENTS, years: tuple[int, int] | None = None,
                sectors=None, issue_size_cr: tuple[float, float] | None = None) -> pd.DataFrame:
    """
    Rows matching every filter. sectors=None keeps all sectors. An issue-size range drops IPOs with no issue size,
    unless it spans the full range of the data.
    """
    keep = df['segment'].isin(segments)
    if years is not None:
        keep &= df['listing_year'].between(*years)
    if sectors is not None:
        keep &= df['sector_label'].isin(sectors)
    if issue_size_cr is not None:
        low, high = issue_size_cr
        full_range = low <= df['issue_size_cr'].min() and high >= df['issue_size_cr'].max()
        if not full_range:
            keep &= df['issue_size_cr'].between(low, high)
    return df[keep.fillna(False)]


def segment_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Headline numbers per segment. Listing figures use IPOs with a listing price; 1-year figures, IPOs with a return."""
    rows = {}
    for segment in SEGMENTS:
        seg = df[df['segment'] == segment]
        gains = seg['listing_gain_pct'].dropna()
        excess = seg['excess_return_1y_pct'].dropna()
        rows[segment] = {
            'ipos': len(seg),
            'with_listing_price': len(gains),
            'median_listing_gain_pct': gains.median(),
            'share_positive': (gains > 0).mean() if len(gains) else np.nan,
            'with_1y_return': len(excess),
            'median_excess_1y_pp': excess.median(),
            'share_beat_nifty_1y': (excess > 0).mean() if len(excess) else np.nan,
        }
    counts = ['ipos', 'with_listing_price', 'with_1y_return']
    return pd.DataFrame.from_dict(rows, orient='index').astype({c: int for c in counts}).rename_axis('segment')


def yearly_base_rates(df: pd.DataFrame) -> pd.DataFrame:
    """IPOs, share opening above issue and median listing gain by segment and listing year"""
    listed = df[df['listing_gain_pct'].notna()]
    grouped = listed.groupby(['segment', 'listing_year'], observed=True)['listing_gain_pct']
    return pd.DataFrame({
        'ipos': grouped.size(),
        'share_positive': grouped.apply(lambda v: (v > 0).mean()),
        'median_listing_gain_pct': grouped.median(),
    }).reset_index()


def subscription_groups(df: pd.DataFrame, max_groups: int = 10, min_per_group: int = 15) -> pd.DataFrame:
    """
    Listing gain by equal-count subscription group, per segment, on the subscription sample. Uses deciles when
    there are enough IPOs, fewer groups when a filter leaves too few; a segment with under two groups' worth is left out.
    """
    tables = []
    sample = df[subscription_sample(df)]
    for segment in SEGMENTS:
        seg = sample[sample['segment'] == segment]
        groups = min(max_groups, len(seg) // min_per_group)
        if groups < 2:
            continue
        table = decile_table(seg, by='subscription_total_x', value='listing_gain_pct', q=groups,
                             at_ceiling='at_listing_ceiling')
        tables.append(table.reset_index().assign(segment=segment, groups=groups))
    columns = ['segment', 'groups', 'group', 'n', 'subscription_total_x_min', 'subscription_total_x_median',
               'subscription_total_x_max', 'median', 'q25', 'q75', 'share_positive', 'share_at_ceiling']
    return pd.concat(tables, ignore_index=True)[columns] if tables else pd.DataFrame(columns=columns)


def lookup_labels(df: pd.DataFrame) -> pd.Series:
    """'Company (TICKER, Segment, listed YYYY-MM-DD)' for each row, sorted by company name"""
    segment = df['segment'].astype(str).map({'MAINBOARD': 'Mainboard', 'SME': 'SME'})
    # CAMS and PROTEAN listed on BSE only, so they have no NSE listing date; fall back to issue close
    listed = ('listed ' + df['listing_dt'].dt.strftime('%Y-%m-%d')).fillna(
        'issue closed ' + df['ipo_end_dt'].dt.strftime('%Y-%m-%d'))
    labels = df['company_name'].str.strip() + ' (' + df['ticker'] + ', ' + segment + ', ' + listed + ')'
    return labels.sort_values(key=lambda s: s.str.upper())


def subscription_breakdown(ipo: pd.Series) -> pd.DataFrame:
    """One IPO's subscription multiple by investor category, with a note where a figure is missing"""
    rows = []
    for column, category in SUBSCRIPTION_CATEGORIES.items():
        value = ipo[column]
        if pd.notna(value):
            note = ''
        elif category == 'QIB' and pd.notna(ipo['has_qib_category']) and not ipo['has_qib_category']:
            note = 'no QIB portion in this issue'
        else:
            note = 'not reported'
        rows.append({'category': category, 'times_subscribed': value, 'note': note})
    return pd.DataFrame(rows)


def return_history(ipo: pd.Series) -> pd.DataFrame:
    """
    One IPO's returns at each horizon: from issue price, NIFTY over the same period, the difference, and the
    difference for a buyer at the listing-day close. status says why a return is missing.
    """
    dates = pd.DataFrame({'listing_dt': [ipo['listing_dt']], 'snapshot_date': [ipo['snapshot_date']]})
    rows = []
    for horizon in HORIZONS:
        value = ipo[f'return_{horizon}_pct']
        if pd.notna(value):
            status = 'ok'
        elif pd.isna(ipo['listing_dt']):
            status = 'No NSE listing date (listed on BSE only)'
        elif not bool(horizon_elapsed(dates, horizon).iloc[0]):
            status = 'Not reached by the snapshot date'
        else:
            status = 'No NSE trade in the 30 days before the horizon date, or an unexplained price gap'
        rows.append({
            'horizon': horizon,
            'return_pct': value,
            'nifty_return_pct': ipo[f'nifty_return_{horizon}_pct'],
            'excess_return_pp': ipo[f'excess_return_{horizon}_pct'],
            'aftermarket_excess_return_pp': ipo[f'aftermarket_excess_return_{horizon}_pct'],
            'status': status,
        })
    return pd.DataFrame(rows)


def percentile_in_segment(df: pd.DataFrame, ipo: pd.Series, column: str) -> float:
    """Share of the IPO's segment with a lower value in column, ties counted half (NaN if the IPO has none)"""
    value = ipo[column]
    if pd.isna(value):
        return np.nan
    peers = df.loc[df['segment'] == ipo['segment'], column].dropna()
    if peers.empty:
        return np.nan
    return float(((peers < value).sum() + 0.5 * (peers == value).sum()) / len(peers))
