"""src/config.py, src/data.py and src/plotting.py against small exports written in a temp folder."""

from datetime import date

import duckdb
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from src.config import available_snapshots, resolve_snapshot
from src.data import at_listing_ceiling, horizon_elapsed, listing_ceiling_pct, load_funnel, load_mart
from src.plotting import asinh_bins, save_fig

matplotlib.use('Agg')


@pytest.fixture
def mart_dir(tmp_path, monkeypatch):
    monkeypatch.delenv('IPO_SNAPSHOT', raising=False)
    directory = tmp_path / 'mart'
    directory.mkdir()
    # Same DuckDB types the pipeline exports: DATE, DECIMAL, nullable BOOLEAN
    for snapshot in ('2026-01-22', '2026-02-15'):
        duckdb.sql(f"""
            SELECT * FROM (VALUES
                ('AAA', 'SME',       DATE '2025-01-31', 105.50::DECIMAL(10,2), TRUE,  12.5, DATE '{snapshot}'),
                ('BBB', 'MAINBOARD', DATE '2025-03-10', 250.00::DECIMAL(10,2), NULL,  NULL, DATE '{snapshot}')
            ) AS t(ticker, segment, listing_dt, issue_price, has_qib_category, listing_gain_pct, snapshot_date)
        """).write_parquet(str(directory / f'mart_ipo_analytics_{snapshot}.parquet'))
    (directory / 'mart_ipo_analytics_2026-02-15.csv').write_text('ignored')
    return directory


def test_snapshots_found_from_parquet_exports(mart_dir):
    assert available_snapshots(mart_dir) == [date(2026, 1, 22), date(2026, 2, 15)]
    assert resolve_snapshot(mart_dir=mart_dir) == date(2026, 2, 15)
    assert resolve_snapshot('2026-01-22', mart_dir=mart_dir) == date(2026, 1, 22)


def test_snapshot_from_environment(mart_dir, monkeypatch):
    monkeypatch.setenv('IPO_SNAPSHOT', '2026-01-22')
    assert resolve_snapshot(mart_dir=mart_dir) == date(2026, 1, 22)


def test_no_exports_raises(tmp_path, monkeypatch):
    monkeypatch.delenv('IPO_SNAPSHOT', raising=False)
    with pytest.raises(FileNotFoundError, match='pipeline.py'):
        resolve_snapshot(mart_dir=tmp_path)


def test_load_mart_converts_duckdb_types(mart_dir):
    df = load_mart(mart_dir=mart_dir)
    assert pd.api.types.is_datetime64_any_dtype(df['listing_dt'])
    assert df['issue_price'].dtype == float
    assert df['has_qib_category'].dtype == 'boolean'
    assert df['has_qib_category'].isna().tolist() == [False, True]
    assert list(df['segment'].cat.categories) == ['MAINBOARD', 'SME']
    assert df['listing_year'].tolist() == [2025, 2025]


def test_horizon_elapsed_uses_calendar_months(mart_dir):
    df = load_mart(mart_dir=mart_dir)
    df['snapshot_date'] = pd.Timestamp('2026-01-31')
    # AAA listed 2025-01-31: one year lands on the snapshot; BBB is still short of a year
    assert horizon_elapsed(df, '1y').tolist() == [True, False]
    assert horizon_elapsed(df, '1m').tolist() == [True, True]


def test_listing_ceiling_periods_and_tick_tolerance():
    df = pd.DataFrame({
        'segment': ['SME', 'SME', 'SME', 'SME', 'MAINBOARD', 'SME'],
        'listing_dt': pd.to_datetime(['2018-03-01', '2018-03-01', '2020-01-01', '2024-07-04', '2024-08-01',
                                      '2025-01-01']),
        'issue_price': [51.0, 51.0, 100.0, 36.0, 100.0, 100.0],
        'listing_open': [61.20, 61.10, 250.0, 68.35, 190.0, np.nan],
    })
    assert listing_ceiling_pct(df).tolist()[:2] == [20, 20]
    assert np.isnan(listing_ceiling_pct(df)[2]) and np.isnan(listing_ceiling_pct(df)[4])
    # 51 x 1.2 = 61.20 (at the ceiling); 61.10 is two ticks below; 36 x 1.9 = 68.40, one tick below counts
    assert at_listing_ceiling(df).tolist() == [True, False, False, True, False, False]


def test_load_funnel_pivots_segments(tmp_path, mart_dir):
    pd.DataFrame({
        'step': [1, 1, 1], 'stage': ['mart_rows'] * 3, 'description': ['Rows'] * 3,
        'segment': ['ALL', 'MAINBOARD', 'SME'], 'n': [3, 1, 2],
    }).to_csv(tmp_path / 'join_funnel_2026-02-15.csv', index=False)
    funnel = load_funnel(reports_dir=tmp_path, mart_dir=mart_dir)
    assert funnel.columns.tolist() == ['step', 'stage', 'description', 'ALL', 'MAINBOARD', 'SME']
    assert funnel.iloc[0][['ALL', 'MAINBOARD', 'SME']].tolist() == [3, 1, 2]


def test_asinh_bins_cover_range_and_are_symmetric():
    edges = asinh_bins([-50, 0, 50], n_bins=4)
    assert edges[0] == pytest.approx(-50) and edges[-1] == pytest.approx(50)
    assert np.allclose(edges, -edges[::-1])


def test_save_fig_writes_png(tmp_path):
    fig, ax = plt.subplots()
    ax.plot([1, 2])
    path = save_fig(fig, 'example', figures_dir=tmp_path / 'figures')
    plt.close(fig)
    assert path == tmp_path / 'figures' / 'example.png'
    assert path.read_bytes()[:4] == b'\x89PNG'
