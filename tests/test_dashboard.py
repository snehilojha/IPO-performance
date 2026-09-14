"""src/dashboard.py on a small hand-built mart, plus every dashboard page run against the committed export."""

import numpy as np
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src import dashboard
from src.config import PROJECT_ROOT, available_snapshots

SNAPSHOT = pd.Timestamp('2026-02-15')
APP = PROJECT_ROOT / 'app' / 'streamlit_app.py'


def ipo(**values):
    row = {
        'ticker': 'AAA', 'company_name': 'Aaa Limited', 'segment': 'MAINBOARD',
        'listing_dt': pd.Timestamp('2024-03-01'), 'ipo_end_dt': pd.Timestamp('2024-02-26'),
        'issue_price': 100.0, 'listing_open': 110.0, 'listing_gain_pct': 10.0,
        'has_listing_price': True, 'has_subscription_figures': True, 'has_qib_category': True,
        'subscription_qib_x': 5.0, 'subscription_nii_x': 4.0, 'subscription_retail_x': 3.0,
        'subscription_total_x': 4.0, 'issue_size_cr': 500.0, 'sector': 'Industrials',
        'excess_return_1y_pct': 5.0, 'snapshot_date': SNAPSHOT,
    }
    for horizon in ('1m', '6m', '1y', '3y'):
        for prefix in ('return', 'nifty_return', 'excess_return', 'aftermarket_excess_return'):
            row.setdefault(f'{prefix}_{horizon}_pct', 1.0)
    row.update(values)
    return row


def make_mart(rows) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df['segment'] = pd.Categorical(df['segment'], categories=('MAINBOARD', 'SME'), ordered=True)
    df['has_qib_category'] = df['has_qib_category'].astype('boolean')
    df['listing_year'] = df['listing_dt'].dt.year.astype('Int64')
    return dashboard.with_derived_columns(df)


def test_subscription_sample_starts_sme_in_2018_and_needs_both_measures():
    mart = make_mart([
        ipo(ticker='MB2016', listing_dt=pd.Timestamp('2016-09-01')),
        ipo(ticker='SME2017', segment='SME', listing_dt=pd.Timestamp('2017-06-01')),
        ipo(ticker='SME2018', segment='SME', listing_dt=pd.Timestamp('2018-01-01')),
        ipo(ticker='NOFIG', has_subscription_figures=False),
        ipo(ticker='NOPRICE', has_listing_price=False),
    ])
    assert mart.loc[mart['in_subscription_sample'], 'ticker'].tolist() == ['MB2016', 'SME2018']


def test_filters_keep_missing_issue_size_only_for_the_full_range():
    mart = make_mart([
        ipo(ticker='SMALL', issue_size_cr=20.0, sector=None),
        ipo(ticker='BIG', issue_size_cr=2_000.0),
        ipo(ticker='UNKNOWN', issue_size_cr=np.nan, listing_dt=pd.Timestamp('2021-05-01')),
    ])
    assert len(dashboard.filter_ipos(mart, issue_size_cr=(0, 30_000))) == 3
    assert dashboard.filter_ipos(mart, issue_size_cr=(0, 100))['ticker'].tolist() == ['SMALL']
    assert dashboard.filter_ipos(mart, sectors=[dashboard.NO_SECTOR])['ticker'].tolist() == ['SMALL']
    assert dashboard.filter_ipos(mart, years=(2021, 2021))['ticker'].tolist() == ['UNKNOWN']
    assert dashboard.filter_ipos(mart, segments=['SME']).empty


def test_segment_kpis_count_flat_listings_as_not_positive():
    mart = make_mart([
        ipo(listing_gain_pct=10.0, excess_return_1y_pct=-5.0),
        ipo(listing_gain_pct=0.0, excess_return_1y_pct=np.nan),
        ipo(listing_gain_pct=np.nan, has_listing_price=False, excess_return_1y_pct=np.nan),
        ipo(segment='SME', listing_gain_pct=-2.0, excess_return_1y_pct=3.0),
    ])
    kpis = dashboard.segment_kpis(mart)
    mainboard = kpis.loc['MAINBOARD']
    assert (mainboard['ipos'], mainboard['with_listing_price'], mainboard['with_1y_return']) == (3, 2, 1)
    assert mainboard['share_positive'] == 0.5
    assert mainboard['median_listing_gain_pct'] == 5.0
    assert kpis.loc['SME', 'share_beat_nifty_1y'] == 1.0


def test_subscription_groups_shrink_with_the_sample_and_skip_thin_segments():
    rows = [ipo(ticker=f'MB{i}', subscription_total_x=float(i + 1), listing_gain_pct=float(i)) for i in range(45)]
    rows += [ipo(ticker=f'SME{i}', segment='SME', subscription_total_x=1.0, listing_gain_pct=1.0) for i in range(20)]
    groups = dashboard.subscription_groups(make_mart(rows))
    assert groups['segment'].unique().tolist() == ['MAINBOARD']
    assert groups['groups'].iloc[0] == 3 and groups['n'].tolist() == [15, 15, 15]
    assert groups['median'].tolist() == [7.0, 22.0, 37.0]


def test_lookup_breakdown_and_history_explain_missing_values():
    mart = make_mart([
        ipo(ticker='SMEOLD', segment='SME', listing_dt=pd.Timestamp('2019-01-01'), has_qib_category=False,
            subscription_qib_x=np.nan, return_3y_pct=np.nan),
        ipo(ticker='NEW', listing_dt=pd.Timestamp('2025-11-03'), subscription_nii_x=np.nan,
            **{f'return_{h}_pct': np.nan for h in ('6m', '1y', '3y')}),
        ipo(ticker='BSE', listing_dt=pd.NaT, return_1m_pct=np.nan),
    ])
    old, new, bse = (mart.iloc[i] for i in range(3))

    assert dashboard.subscription_breakdown(old)['note'].tolist() == ['no QIB portion in this issue', '', '', '']
    assert dashboard.subscription_breakdown(new)['note'].tolist() == ['', 'not reported', '', '']

    assert dashboard.return_history(old)['status'].iloc[3].startswith('No NSE trade')
    assert dashboard.return_history(new)['status'].tolist()[1:] == ['Not reached by the snapshot date'] * 3
    assert dashboard.return_history(bse)['status'].iloc[0].startswith('No NSE listing date')

    labels = dashboard.lookup_labels(mart)
    assert labels.loc[2] == 'Aaa Limited (BSE, Mainboard, issue closed 2024-02-26)'
    assert labels.is_unique


def test_percentile_in_segment_counts_ties_half():
    mart = make_mart([ipo(listing_gain_pct=g) for g in (0.0, 10.0, 10.0, 30.0)]
                     + [ipo(segment='SME', listing_gain_pct=100.0)])
    assert dashboard.percentile_in_segment(mart, mart.iloc[1], 'listing_gain_pct') == 0.5
    assert np.isnan(dashboard.percentile_in_segment(mart, pd.Series({'segment': 'SME', 'x': np.nan}), 'x'))


@pytest.mark.skipif(not available_snapshots(), reason='no mart export in data/mart')
@pytest.mark.parametrize('page', ['overview', 'explore', 'lookup', 'findings'])
def test_dashboard_page_runs_without_errors(page):
    app = AppTest.from_file(str(APP), default_timeout=60)
    app.run()
    if page != 'overview':
        app.switch_page(f'views/{page}.py')
        app.run()
    if page == 'lookup':
        app.selectbox[0].select_index(0).run()
    assert not app.exception, [e.value for e in app.exception]
    assert app.title, f'{page} rendered no title'
