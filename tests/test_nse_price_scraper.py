"""Parsing and security-linking in scrapers/nse_price_scraper.py, on small inline samples (no network)."""

from datetime import date

import pandas as pd
import pytest

from scrapers.nse_price_scraper import (
    NSEPriceScraper,
    cm_bhavcopy_url,
    parse_cm_bhavcopy,
    parse_index_close,
    parse_udiff_bhavcopy,
    udiff_bhavcopy_url,
)

CM_SAMPLE = """SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
SALASAR,BE,250,262.5,250,262.5,262.5,108,729832,185638629,25-JUL-2017,4688,INE170V01019,
ZOTA,SM,134.4,134.5,133.75,134.2,134.45,134.05,19000,2548100,25-JUL-2017,19,INE358U01012,
1018GS2026,GS,115,115,113.66,113.66,113.66,117.6,172,19550,25-JUL-2017,4,IN0020160019,
"""

CM_SAMPLE_SHORT_YEAR = CM_SAMPLE.replace('25-JUL-2017', '13-Jul-20')

UDIFF_SAMPLE = (
    "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,"
    "OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,"
    "ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4\n"
    "2024-10-04,2024-10-04,CM,NSE,STK,1,INE0I3Y01014,BMETRICS,SM,,,,,BHARAT METRICS,71.45,72,70,71.50,71.5,"
    "136.15,,71.5,,,1000,71500,12,F1,1000,,,,,\n"
    "2024-10-04,2024-10-04,CM,NSE,STK,2,INE002A01018,RELIANCE,EQ,,,,,RELIANCE,2900,2950,2890,2940,2940,"
    "2895,,2940,,,5000000,1.47e10,90000,F1,1,,,,,\n"
    "2024-10-04,2024-10-04,CM,NSE,ETF,3,INF204KB14I2,NIFTYBEES,EQ,,,,,NIPPON ETF,270,272,269,271,271,"
    "270,,271,,,90000,2.4e7,900,F1,1,,,,,\n"
)

INDEX_SAMPLE = """Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield
Nifty 50,13-02-2026,25571.15,25630.35,25444.3,25471.1,-336.1,-1.3,453523192,40137.15,22.24,3.46,1.21
Nifty Next 50,13-02-2026,68000,68100,67500,67600,-400,-0.6,1,1,1,1,1
"""


def test_cm_bhavcopy_keeps_equity_series_only():
    df = parse_cm_bhavcopy(CM_SAMPLE)
    assert list(df['symbol']) == ['SALASAR', 'ZOTA']
    salasar = df.iloc[0]
    assert salasar['trade_dt'] == date(2017, 7, 25)
    assert (salasar['open'], salasar['close'], salasar['prev_close']) == (250, 262.5, 108)
    assert salasar['isin'] == 'INE170V01019'


def test_cm_bhavcopy_two_digit_year():
    assert set(parse_cm_bhavcopy(CM_SAMPLE_SHORT_YEAR)['trade_dt']) == {date(2020, 7, 13)}


def test_udiff_bhavcopy_keeps_stocks_only():
    df = parse_udiff_bhavcopy(UDIFF_SAMPLE)
    assert list(df['symbol']) == ['BMETRICS', 'RELIANCE']
    bmetrics = df.iloc[0]
    assert bmetrics['trade_dt'] == date(2024, 10, 4)
    assert (bmetrics['open'], bmetrics['close'], bmetrics['prev_close']) == (71.45, 71.5, 136.15)
    assert bmetrics['series'] == 'SM'


def test_index_close_picks_nifty_50():
    row = parse_index_close(INDEX_SAMPLE)
    assert row == {'trade_dt': date(2026, 2, 13), 'open': 25571.15, 'high': 25630.35, 'low': 25444.3,
                   'close': 25471.1}


def test_archive_urls_by_format():
    assert cm_bhavcopy_url(date(2017, 7, 25)).endswith('/EQUITIES/2017/JUL/cm25JUL2017bhav.csv.zip')
    assert udiff_bhavcopy_url(date(2024, 10, 4)).endswith('/BhavCopy_NSE_CM_0_0_0_20241004_F_0000.csv.zip')


def test_linked_prices_follow_renames_splits_and_new_listings(tmp_path):
    raw_dir = tmp_path / 'raw'
    cache_dir = tmp_path / 'cache'
    (cache_dir / 'bhavcopy').mkdir(parents=True)
    raw_dir.mkdir()
    pd.DataFrame({
        'symbol': ['ZOMATO', 'DCX', 'OLDCO'],
        'ipoenddate': ['16-JUL-2021', '02-NOV-2022', '01-JAN-2017'],
        'pricerange': ['Rs.72 to Rs.76', 'Rs.197 to Rs.207', 'Rs.10'],
        'issueprice': ['76', '207', '10'],
        'securitytype': ['EQ', 'EQ', 'EQ'],
    }).to_csv(raw_dir / 'ipo_metadata.csv', index=False)

    def day(trade_dt, rows):
        frame = pd.DataFrame(rows, columns=['symbol', 'series', 'isin', 'prev_close'])
        frame['trade_dt'] = trade_dt
        for column in ('open', 'high', 'low', 'close'):
            frame[column] = frame['prev_close']
        frame['volume'] = 1
        frame['trades'] = 1
        frame.to_parquet(cache_dir / 'bhavcopy' / f'{trade_dt}.parquet', index=False)

    day(date(2016, 2, 1), [('UNRELATED', 'EQ', 'INE000X01011', 50.0), ('OLDCO', 'EQ', 'INE111A01011', 10.0)])
    day(date(2021, 7, 23), [('ZOMATO', 'EQ', 'INE758T01015', 76.0), ('UNRELATED', 'EQ', 'INE000X01011', 50.0)])
    # renamed (same ISIN); a stranger lists at a price no IPO had; DCX lists as DCXINDIA at its issue price
    day(date(2022, 11, 11), [('ETERNAL', 'EQ', 'INE758T01015', 90.0), ('STRANGER', 'EQ', 'INE999Z01011', 33.0),
                             ('DCXINDIA', 'EQ', 'INE0KL801015', 207.0)])
    # face-value split: same symbol, new ISIN
    day(date(2023, 1, 2), [('DCXINDIA', 'EQ', 'INE0KL801023', 150.0)])

    scraper = NSEPriceScraper(date(2026, 2, 15), raw_dir=raw_dir, cache_dir=cache_dir)
    symbols, start = scraper.read_ipo_symbols()
    assert start == date(2017, 1, 1)

    linked = scraper.linked_prices(symbols)
    assert set(linked['symbol']) == {'ZOMATO', 'ETERNAL', 'DCXINDIA', 'OLDCO'}
    assert set(linked.loc[linked['symbol'] == 'DCXINDIA', 'isin']) == {'INE0KL801015', 'INE0KL801023'}
    assert list(linked.columns) == ['trade_dt', 'symbol', 'series', 'isin', 'open', 'close', 'prev_close', 'volume']


@pytest.mark.parametrize('code', ['EQ', 'BE', 'Equity', 'SME'])
def test_read_ipo_symbols_accepts_every_equity_code_in_the_seed(tmp_path, code):
    """
    The scraper decides which symbols to download prices for. It reads the equity codes from
    data/seeds/security_types.csv, so a feed rename (EQ and BE became EQUITY during 2026) cannot
    quietly leave a whole segment without listing prices.
    """
    raw_dir = tmp_path / 'raw'
    raw_dir.mkdir()
    pd.DataFrame({
        'symbol': ['ZOMATO', 'SOMENCD'],
        'ipoenddate': ['16-JUL-2021', '10-MAR-2024'],
        'pricerange': ['Rs.72 to Rs.76', 'Rs.1000'],
        'issueprice': ['76', '1000'],
        'securitytype': [code, 'N0'],
    }).to_csv(raw_dir / 'ipo_metadata.csv', index=False)

    scraper = NSEPriceScraper(date(2026, 2, 15), raw_dir=raw_dir, cache_dir=tmp_path / 'cache')
    symbols, start = scraper.read_ipo_symbols()

    assert symbols == ['ZOMATO']       # the NCD is left out
    assert start == date(2021, 7, 16)
