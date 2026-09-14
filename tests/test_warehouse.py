"""Builds the warehouse (stages 2-5 + export) from the small CSVs in tests/fixtures/ and checks the edge cases."""

from datetime import date
from pathlib import Path

import duckdb
import pytest

from pipeline import DataQualityError, IPODataPipeline

FIXTURES = Path(__file__).parent / 'fixtures'
SNAPSHOT = date(2026, 2, 15)
FIXTURE_THRESHOLDS = {
    'min_mart_rows': 5,
    'min_subscription_coverage': 0.5,
    'min_market_coverage': 0.5,
}


def make_pipeline(out_dir, thresholds=None, snapshot=SNAPSHOT):
    paths = {
        'raw_data_dir': FIXTURES / 'raw',
        'seeds_dir': FIXTURES / 'seeds',
        'duckdb_path': out_dir / 'ipo.duckdb',
        'mart_dir': out_dir / 'mart',
        'reports_dir': out_dir / 'reports',
    }
    return IPODataPipeline(snapshot, skip_scrape=True, paths=paths,
                           thresholds={**FIXTURE_THRESHOLDS, **(thresholds or {})})


@pytest.fixture(scope='module')
def build_dir(tmp_path_factory):
    out_dir = tmp_path_factory.mktemp('warehouse')
    make_pipeline(out_dir).run()
    return out_dir


@pytest.fixture(scope='module')
def db(build_dir):
    con = duckdb.connect(str(build_dir / 'ipo.duckdb'), read_only=True)
    yield con
    con.close()


def mart_row(db, ticker):
    result = db.execute("SELECT * FROM mart_ipo_analytics WHERE ticker = ?", [ticker])
    columns = [col[0] for col in result.description]
    rows = result.fetchall()
    assert len(rows) == 1, f"expected one mart row for {ticker}, got {len(rows)}"
    return dict(zip(columns, rows[0]))


def test_exports_written(build_dir):
    assert (build_dir / 'mart' / 'mart_ipo_analytics_2026-02-15.parquet').exists()
    assert (build_dir / 'mart' / 'mart_ipo_analytics_2026-02-15.csv').exists()
    assert (build_dir / 'reports' / 'join_funnel_2026-02-15.csv').exists()


def test_mart_has_every_listed_ipo(db):
    tickers = {row[0] for row in db.execute("SELECT ticker FROM mart_ipo_analytics").fetchall()}
    assert tickers == {'GSTL', 'ALPHA', 'GOEL', 'MIGR', 'FOLL', 'FORGE', 'ZOMATO', 'DRL', 'TMB'}


def test_ticker_collision_resolves_to_right_company(db):
    row = mart_row(db, 'GSTL')
    assert row['tickertape_sid'] == 'GSTL'
    assert row['current_company_name'] == 'Globesecure Technologies Ltd'


def test_second_subscription_layout_parses_and_joins(db):
    start, end = db.execute(
        "SELECT ipo_start_dt, ipo_end_dt FROM stg_ipo_subscription WHERE company_name = 'Goel Food Products Limited'"
    ).fetchone()
    assert (start, end) == (date(2022, 6, 15), date(2022, 6, 20))

    row = mart_row(db, 'GOEL')
    assert row['has_subscription_data']
    assert row['subscription_total_x'] == 1.64
    assert row['applications'] == 211219
    assert row['has_qib_category'] is False


def test_non_equity_and_non_ipo_issues_excluded(db):
    security_types = {row[0] for row in db.execute("SELECT DISTINCT security_type FROM stg_past_ipo").fetchall()}
    assert security_types <= {'EQ', 'BE', 'SME'}

    reasons = dict(db.execute(
        "SELECT ticker, exclusion_reason FROM stg_past_ipo WHERE exclusion_reason IS NOT NULL"
    ).fetchall())
    assert reasons == {'IDEAFPO': 'fpo', 'PKH': 'withdrawn', 'PARTPP': 'partly_paid', 'FOLL': 'follow_on'}


def test_no_duplicate_ipos(db):
    duplicates = db.execute("""
        SELECT ticker, ipo_end_dt FROM mart_ipo_analytics GROUP BY ALL HAVING COUNT(*) > 1
    """).fetchall()
    assert duplicates == []


def test_follow_on_keeps_first_issue(db):
    row = mart_row(db, 'FOLL')
    assert row['ipo_end_dt'] == date(2017, 1, 27)
    assert row['issue_price'] == 81


def test_migration_listing_date_is_not_used(db):
    row = mart_row(db, 'MIGR')
    assert row['listing_dt'] is None
    assert row['listing_dt_reported'] == date(2023, 7, 5)
    assert row['listing_dt_flag'] == 'listing_date_is_later_event'
    # issueprice 265 is outside the band of 100, so the band is used
    assert row['issue_price'] == 100
    assert row['issue_price_source'] == 'band_upper'


def test_issue_price_inside_band_is_kept(db):
    row = mart_row(db, 'TMB')
    assert row['issue_price'] == 510
    assert row['issue_price_source'] == 'feed_issue_price'


def test_fuzzy_subscription_match_prefers_best_candidate(db):
    row = mart_row(db, 'ALPHA')
    assert row['subscription_match_method'] == 'exact'
    assert row['subscription_total_x'] == 30.0


def test_subscription_match_tolerates_close_date_offset(db):
    row = mart_row(db, 'FORGE')
    assert row['subscription_match_method'] == 'fuzzy'
    assert row['subscription_total_x'] == 6.5


def test_market_match_methods(db):
    assert mart_row(db, 'ZOMATO')['market_match_method'] == 'manual'
    assert mart_row(db, 'ZOMATO')['current_ticker'] == 'ETERNAL'
    assert mart_row(db, 'FORGE')['market_match_method'] == 'name'
    assert mart_row(db, 'FORGE')['current_ticker'] == 'TIRUPATIFL'
    assert mart_row(db, 'DRL')['has_market_data'] is False


def test_missing_ratios_stay_null(db):
    row = mart_row(db, 'GOEL')
    assert row['pe_ratio'] is None
    assert row['is_lossmaking'] is None


def test_returns_and_holding_period(db):
    row = mart_row(db, 'TMB')
    assert row['holding_days'] == (SNAPSHOT - date(2022, 9, 7)).days
    assert row['return_since_issue_pct'] == pytest.approx((649.65 - 510) / 510 * 100, abs=0.01)
    assert row['annualized_return_pct'] is not None


def test_failed_check_stops_pipeline(tmp_path):
    with pytest.raises(DataQualityError, match='mart_min_rows'):
        make_pipeline(tmp_path, thresholds={'min_mart_rows': 1000}).run()
    assert not (tmp_path / 'mart' / 'mart_ipo_analytics_2026-02-15.parquet').exists()


def test_missing_snapshot_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match='2026-02-15'):
        make_pipeline(tmp_path, snapshot=date(2020, 1, 1)).run()
