"""Builds the warehouse (stages 2-5 + export) from the small files in tests/fixtures/ and checks the edge cases."""

import shutil
from datetime import date, timedelta
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
    'min_listing_price_coverage_mainboard': 0.5,
    'min_listing_price_coverage_sme': 0.5,
    'max_unexplained_price_gaps': 1,
}


def nifty(day):
    """Value of the fixture NIFTY 50 series on the last weekday on or before `day` (see nifty50.csv)"""
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return 8000 + 6 * (day - date(2016, 1, 1)).days


def pct(end, start):
    return (end / start - 1) * 100


@pytest.fixture(scope='module')
def raw_root(tmp_path_factory):
    """Copy of the fixture snapshot with the price CSV written as the Parquet file the pipeline reads"""
    root = tmp_path_factory.mktemp('raw')
    snapshot_dir = root / SNAPSHOT.isoformat()
    shutil.copytree(FIXTURES / 'raw' / SNAPSHOT.isoformat(), snapshot_dir)
    prices_csv = snapshot_dir / 'nse_prices.csv'
    duckdb.execute(f"""
        COPY (
            SELECT trade_dt::DATE AS trade_dt, symbol, series, isin, open::DOUBLE AS open, close::DOUBLE AS close,
                   prev_close::DOUBLE AS prev_close, volume::BIGINT AS volume
            FROM read_csv('{prices_csv.as_posix()}', all_varchar=true)
        ) TO '{(snapshot_dir / 'nse_prices.parquet').as_posix()}' (FORMAT PARQUET)
    """)
    prices_csv.unlink()
    return root


def make_pipeline(out_dir, raw_root, thresholds=None, snapshot=SNAPSHOT):
    paths = {
        'raw_data_dir': raw_root,
        'seeds_dir': FIXTURES / 'seeds',
        'duckdb_path': out_dir / 'ipo.duckdb',
        'mart_dir': out_dir / 'mart',
        'reports_dir': out_dir / 'reports',
    }
    return IPODataPipeline(snapshot, skip_scrape=True, paths=paths,
                           thresholds={**FIXTURE_THRESHOLDS, **(thresholds or {})})


@pytest.fixture(scope='module')
def build_dir(tmp_path_factory, raw_root):
    out_dir = tmp_path_factory.mktemp('warehouse')
    make_pipeline(out_dir, raw_root).run()
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


# ---- Phase 1: sources, matching, exclusions --------------------------------------------------------------

def test_exports_written(build_dir):
    assert (build_dir / 'mart' / 'mart_ipo_analytics_2026-02-15.parquet').exists()
    assert (build_dir / 'mart' / 'mart_ipo_analytics_2026-02-15.csv').exists()
    assert (build_dir / 'reports' / 'join_funnel_2026-02-15.csv').exists()


def test_mart_has_every_listed_ipo(db):
    tickers = {row[0] for row in db.execute("SELECT ticker FROM mart_ipo_analytics").fetchall()}
    assert tickers == {'GSTL', 'ALPHA', 'GOEL', 'MIGR', 'FOLL', 'FORGE', 'ZOMATO', 'DRL', 'TMB',
                       'NEWSYM', 'CAPD', 'BSEONLY'}


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
    assert row['has_subscription_figures']


def test_subscription_record_without_figures(db):
    # Chittorgarh lists most 2016-17 SME issues with no subscription numbers at all
    row = mart_row(db, 'DRL')
    assert row['has_subscription_data']
    assert row['issue_size_cr'] == 4.5
    assert row['has_subscription_figures'] is False
    assert row['has_qib_category'] is None


def test_placeholder_subscription_multiple_is_null(db):
    # Chittorgarh shows 9999 for a QIB figure it doesn't have (Salasar Techno, 2017); the category still exists
    row = mart_row(db, 'FORGE')
    assert row['subscription_qib_x'] is None
    assert row['has_qib_category'] is True
    assert row['subscription_total_x'] == 6.5


def test_non_equity_and_non_ipo_issues_excluded(db):
    security_types = {row[0] for row in db.execute("SELECT DISTINCT security_type FROM stg_past_ipo").fetchall()}
    assert security_types <= {'EQ', 'BE', 'SME'}

    reasons = dict(db.execute(
        "SELECT ticker, exclusion_reason FROM stg_past_ipo WHERE exclusion_reason IS NOT NULL"
    ).fetchall())
    # TMB's second row is an exact duplicate in the feed (as in the 2026-01-30 scrape)
    assert reasons == {'IDEAFPO': 'fpo', 'PKH': 'withdrawn', 'PARTPP': 'partly_paid', 'FOLL': 'follow_on',
                       'TMB': 'follow_on'}


def test_no_duplicate_ipos(db):
    duplicates = db.execute("""
        SELECT ticker, ipo_end_dt FROM mart_ipo_analytics GROUP BY ALL HAVING COUNT(*) > 1
    """).fetchall()
    assert duplicates == []


def test_follow_on_keeps_first_issue(db):
    row = mart_row(db, 'FOLL')
    assert row['ipo_end_dt'] == date(2017, 1, 27)
    assert row['issue_price'] == 81
    assert row['listing_dt'] == date(2017, 2, 6)


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
    # renamed on NSE (same ISIN), found through the price history
    assert mart_row(db, 'FORGE')['market_match_method'] == 'nse_symbol'
    assert mart_row(db, 'FORGE')['current_ticker'] == 'TIRUPATIFL'
    # new symbol and new ISIN on migration, linked by the ISIN issuer prefix
    assert mart_row(db, 'MIGR')['market_match_method'] == 'nse_symbol'
    assert mart_row(db, 'MIGR')['current_ticker'] == 'MIGRIND'
    # no longer trading, so only the identical name can match
    assert mart_row(db, 'GOEL')['market_match_method'] == 'name'
    assert mart_row(db, 'DRL')['has_market_data'] is False


def test_missing_ratios_stay_null(db):
    row = mart_row(db, 'GOEL')
    assert row['pe_ratio'] is None
    assert row['is_lossmaking'] is None


# ---- Phase 2: listing day ------------------------------------------------------------------------------

def test_listing_prices_from_bhavcopy(db):
    row = mart_row(db, 'TMB')
    assert row['has_listing_price']
    assert row['listing_dt'] == date(2022, 9, 15)
    assert row['listing_dt_source'] == 'nse_bhavcopy'
    assert row['listing_method'] == 'feed_symbol'
    assert row['listing_base_matches_issue_price']
    assert row['listing_gain_pct'] == pytest.approx(0.0)
    assert row['listing_day_close_gain_pct'] == pytest.approx(pct(525, 510), abs=0.01)


def test_migration_listing_date_recovered_from_prices(db):
    row = mart_row(db, 'MIGR')
    assert row['listing_dt_reported'] == date(2023, 7, 5)
    assert row['listing_dt_flag'] == 'listing_date_is_later_event'
    assert row['listing_dt'] == date(2018, 3, 28)
    assert row['listing_dt_source'] == 'nse_bhavcopy'
    # issueprice 265 is outside the band of 100, so the band is used — and the listing base price agrees
    assert row['issue_price'] == 100
    assert row['issue_price_source'] == 'band_upper'
    assert row['listing_gain_pct'] == pytest.approx(10.0)


def test_listing_under_a_different_symbol(db):
    row = mart_row(db, 'NEWSYM')
    assert row['listing_dt_reported'] is None
    assert row['listing_method'] == 'new_listing'
    assert row['listing_symbol'] == 'NEWSYMIND'
    assert row['listing_dt'] == date(2024, 11, 11)
    assert row['listing_gain_pct'] == pytest.approx(pct(287, 207), abs=0.01)


def test_rejected_listing_keeps_feed_date_without_prices(db):
    row = mart_row(db, 'BSEONLY')
    assert row['has_listing_price'] is False
    assert row['listing_dt'] == date(2020, 10, 1)
    assert row['listing_dt_source'] == 'nse_feed'
    assert row['listing_gain_pct'] is None
    assert row['return_1y_pct'] is None


def test_issue_price_reconciled_with_listing_base_price(db):
    row = mart_row(db, 'ALPHA')
    # feed says 105 (top of a 100-105 band); the exchange's listing base price is 104
    assert row['issue_price'] == 104
    assert row['issue_price_source'] == 'listing_base_price'
    assert row['listing_base_matches_issue_price']


def test_sme_listing_cap(db):
    capped = mart_row(db, 'CAPD')
    assert capped['listing_cap_applies'] and capped['listing_capped']
    assert capped['listing_gain_pct'] == pytest.approx(90.0)

    before_cap = mart_row(db, 'GSTL')
    assert before_cap['listing_cap_applies'] is False
    assert before_cap['listing_capped'] is None

    assert mart_row(db, 'TMB')['listing_cap_applies'] is None


# ---- Phase 2: adjusted returns ---------------------------------------------------------------------------

def test_horizon_returns_against_nifty(db):
    row = mart_row(db, 'TMB')
    ipo_close, listing = date(2022, 9, 7), date(2022, 9, 15)

    # 1m horizon 2022-10-15 is a Saturday: the Friday close is used
    stock_1m = pct(545, 510)
    nifty_1m = pct(nifty(date(2022, 10, 14)), nifty(ipo_close))
    assert row['return_1m_pct'] == pytest.approx(stock_1m, abs=0.01)
    assert row['nifty_return_1m_pct'] == pytest.approx(nifty_1m, abs=0.01)
    assert row['excess_return_1m_pct'] == pytest.approx(stock_1m - nifty_1m, abs=0.01)

    aftermarket_1y = pct(600, 525) - pct(nifty(date(2023, 9, 15)), nifty(listing))
    assert row['aftermarket_excess_return_1y_pct'] == pytest.approx(aftermarket_1y, abs=0.01)


def test_bonus_adjusts_later_prices(db):
    row = mart_row(db, 'TMB')
    assert row['corporate_action_count'] == 1
    assert row['share_multiplier'] == pytest.approx(2.0)
    assert row['return_3y_pct'] == pytest.approx(pct(460 * 2, 510), abs=0.01)
    assert row['return_since_issue_pct'] == pytest.approx(pct(649.65 * 2, 510), abs=0.01)
    assert row['last_trade_dt'] == date(2026, 2, 13)
    assert row['holding_days'] == (date(2026, 2, 13) - date(2022, 9, 7)).days
    assert row['annualized_return_pct'] is not None


def test_split_filed_under_later_symbol_is_applied(db):
    row = mart_row(db, 'FORGE')
    assert row['share_multiplier'] == pytest.approx(2.0)
    assert row['return_since_issue_pct'] == pytest.approx(pct(63 * 2, 42), abs=0.01)


def test_non_equity_bonus_is_ignored(db):
    row = mart_row(db, 'ZOMATO')
    assert row['corporate_action_count'] == 0
    assert row['return_since_issue_pct'] == pytest.approx(pct(285.2, 76), abs=0.01)


def test_unexplained_gap_blocks_returns_after_it(db):
    row = mart_row(db, 'ALPHA')
    assert row['price_gap_dt'] == date(2024, 3, 15)
    # 6m horizon (2023-12-12) is before the gap, 1y (2024-06-12) after it
    assert row['return_6m_pct'] == pytest.approx(pct(180, 104), abs=0.01)
    assert row['return_1y_pct'] is None
    assert row['return_since_issue_pct'] is None
    assert row['annualized_return_pct'] is None
    # 3 years after listing is after the snapshot
    assert row['return_3y_pct'] is None


def test_stale_price_gives_no_return(db):
    row = mart_row(db, 'GOEL')
    assert row['return_1y_pct'] == pytest.approx(pct(61, 72), abs=0.01)
    assert row['is_trading_at_snapshot'] is False
    # last trade 2024-05-10 is more than 30 days before the 3y horizon (2025-06-28)
    assert row['return_3y_pct'] is None


def test_funnel_reports_price_coverage(db):
    steps = dict(db.execute(
        "SELECT stage, n FROM audit_join_funnel WHERE segment = 'ALL'"
    ).fetchall())
    assert steps['mart_rows'] == 12
    assert steps['with_listing_price'] == 11
    assert steps['with_subscription_data'] == 8
    assert steps['with_subscription_figures'] == 7


# ---- pipeline behaviour ----------------------------------------------------------------------------------

def test_failed_check_stops_pipeline(tmp_path, raw_root):
    with pytest.raises(DataQualityError, match='mart_min_rows'):
        make_pipeline(tmp_path, raw_root, thresholds={'min_mart_rows': 1000}).run()
    assert not (tmp_path / 'mart' / 'mart_ipo_analytics_2026-02-15.parquet').exists()


def test_price_coverage_check_fails_when_listing_prices_missing(tmp_path, raw_root):
    # BSEONLY has no NSE listing prices, so mainboard coverage is below 100%
    with pytest.raises(DataQualityError, match='prices_listing_coverage'):
        make_pipeline(tmp_path, raw_root, thresholds={'min_listing_price_coverage_mainboard': 1.0}).run()


def test_missing_snapshot_raises(tmp_path, raw_root):
    with pytest.raises(FileNotFoundError, match='2020-01-01'):
        make_pipeline(tmp_path, raw_root, snapshot=date(2020, 1, 1)).run()
