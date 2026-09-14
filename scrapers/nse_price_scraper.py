"""
NSE price history for every IPO in a snapshot.

Sources (all public NSE archives):
- CM bhavcopy: one file per trading day with the unadjusted open/high/low/close and previous close of
  every listed security, SME included. Old format until 2024-07-05, UDiFF format afterwards.
- Index closing file: one file per trading day; the NIFTY 50 row is kept.
- Corporate actions API: bonus, split and consolidation announcements, used to adjust prices.

Each day's download is cached in data/cache/nse/ so a rerun only fetches days it doesn't have.

Writes to data/raw/<snapshot>/:
  nse_prices.parquet       daily rows for the securities linked to the snapshot's IPO symbols
  nifty50.csv              NIFTY 50 daily OHLC
  corporate_actions.csv    bonus / split / consolidation rows

Usage:
    python -m scrapers.nse_price_scraper --snapshot 2026-02-15
"""

import argparse
import io
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import requests

from scrapers import IST

PROJECT_ROOT = Path(__file__).resolve().parent.parent

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

ARCHIVE = "https://nsearchives.nseindia.com"
UDIFF_START = date(2024, 7, 8)
CORPORATE_ACTIONS_PAGE = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
CORPORATE_ACTIONS_API = "https://www.nseindia.com/api/corporates-corporateActions"

# Series an equity trades under: normal, trade-for-trade surveillance, and the SME platform
EQUITY_SERIES = ('EQ', 'BE', 'BZ', 'SM', 'ST', 'SZ')
IPO_SECURITY_TYPES = ('EQ', 'BE', 'SME')
CORPORATE_ACTION_PATTERN = r'(?i)bonus|split|sub-division|consolidat'

PRICE_COLUMNS = ['trade_dt', 'symbol', 'series', 'isin', 'open', 'high', 'low', 'close', 'prev_close',
                 'volume', 'trades']
# The snapshot file keeps only what the warehouse uses; the cache keeps every column
SNAPSHOT_PRICE_COLUMNS = ['trade_dt', 'symbol', 'series', 'isin', 'open', 'close', 'prev_close', 'volume']


def cm_bhavcopy_url(day: date) -> str:
    return (f"{ARCHIVE}/content/historical/EQUITIES/{day.year}/{day.strftime('%b').upper()}/"
            f"cm{day.strftime('%d%b%Y').upper()}bhav.csv.zip")


def udiff_bhavcopy_url(day: date) -> str:
    return f"{ARCHIVE}/content/cm/BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip"


def index_close_url(day: date) -> str:
    return f"{ARCHIVE}/content/indices/ind_close_all_{day:%d%m%Y}.csv"


def parse_cm_bhavcopy(text: str) -> pd.DataFrame:
    """Old-format CM bhavcopy (until July 2024)"""
    df = pd.read_csv(io.StringIO(text), dtype=str).rename(columns=str.strip)
    df = df.rename(columns={
        'SYMBOL': 'symbol', 'SERIES': 'series', 'ISIN': 'isin', 'OPEN': 'open', 'HIGH': 'high',
        'LOW': 'low', 'CLOSE': 'close', 'PREVCLOSE': 'prev_close', 'TOTTRDQTY': 'volume',
        'TOTALTRADES': 'trades', 'TIMESTAMP': 'trade_dt',
    })
    # Most files use 13-JUL-2020, some 13-Jul-20
    dates = df['trade_dt'].str.strip()
    parsed = pd.to_datetime(dates, format='%d-%b-%Y', errors='coerce')
    df['trade_dt'] = parsed.fillna(pd.to_datetime(dates, format='%d-%b-%y', errors='coerce')).dt.date
    if df['trade_dt'].isna().any():
        raise ValueError(f"Unparsed bhavcopy dates: {dates[df['trade_dt'].isna()].unique()[:3]}")
    return _finish_prices(df)


def parse_udiff_bhavcopy(text: str) -> pd.DataFrame:
    """UDiFF CM bhavcopy (from July 2024)"""
    df = pd.read_csv(io.StringIO(text), dtype=str)
    df = df[df['FinInstrmTp'].str.strip() == 'STK']
    df = df.rename(columns={
        'TckrSymb': 'symbol', 'SctySrs': 'series', 'ISIN': 'isin', 'OpnPric': 'open', 'HghPric': 'high',
        'LwPric': 'low', 'ClsPric': 'close', 'PrvsClsgPric': 'prev_close', 'TtlTradgVol': 'volume',
        'TtlNbOfTxsExctd': 'trades', 'TradDt': 'trade_dt',
    })
    df['trade_dt'] = pd.to_datetime(df['trade_dt'].str.strip(), format='%Y-%m-%d').dt.date
    return _finish_prices(df)


def _finish_prices(df: pd.DataFrame) -> pd.DataFrame:
    for column in ('symbol', 'series', 'isin'):
        df[column] = df[column].str.strip()
    df = df[df['series'].isin(EQUITY_SERIES)].copy()
    for column in ('open', 'high', 'low', 'close', 'prev_close'):
        df[column] = pd.to_numeric(df[column], errors='coerce')
    for column in ('volume', 'trades'):
        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
    return df[PRICE_COLUMNS].reset_index(drop=True)


def parse_index_close(text: str, index_name: str = 'Nifty 50') -> dict | None:
    df = pd.read_csv(io.StringIO(text), dtype=str).rename(columns=str.strip)
    row = df[df['Index Name'].str.strip().str.lower() == index_name.lower()]
    if row.empty:
        return None
    row = row.iloc[0]
    return {
        'trade_dt': pd.to_datetime(row['Index Date'].strip(), format='%d-%m-%Y').date(),
        'open': float(row['Open Index Value']),
        'high': float(row['High Index Value']),
        'low': float(row['Low Index Value']),
        'close': float(row['Closing Index Value']),
    }


class NSEPriceScraper:
    """Downloads bhavcopies, NIFTY 50 closes and corporate actions for the IPOs in a snapshot."""

    def __init__(self, snapshot: date, raw_dir: Path | None = None, cache_dir: Path | None = None,
                 workers: int = 4, retries: int = 4):
        self.snapshot = snapshot
        self.raw_dir = raw_dir or PROJECT_ROOT / 'data' / 'raw' / snapshot.isoformat()
        self.cache_dir = cache_dir or PROJECT_ROOT / 'data' / 'cache' / 'nse'
        self.workers = workers
        self.retries = retries
        self._local = threading.local()

    # ---- public API ------------------------------------------------------------------------------

    def scrape(self) -> dict[str, pd.DataFrame]:
        ipo_symbols, start = self.read_ipo_symbols()
        days = [start + timedelta(days=n) for n in range((self.snapshot - start).days + 1)]
        days = [d for d in days if d.weekday() < 5]
        print(f"Fetching {len(days)} weekdays of NSE bhavcopy and index files ({start} to {self.snapshot})...")

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for n, _ in enumerate(pool.map(self.cache_day, days), start=1):
                if n % 250 == 0:
                    print(f"  {n}/{len(days)} days")

        return {
            'nse_prices.parquet': self.linked_prices(ipo_symbols),
            'nifty50.csv': self.nifty50(days),
            'corporate_actions.csv': self.corporate_actions(start),
        }

    def save(self, outputs: dict[str, pd.DataFrame]):
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        for filename, df in outputs.items():
            path = self.raw_dir / filename
            if path.exists():
                raise FileExistsError(f"{path} already exists and will not be overwritten")
            if filename.endswith('.parquet'):
                df.to_parquet(path, index=False, compression='zstd')
            else:
                df.to_csv(path, index=False)
            print(f"Saved {len(df)} rows to {path}")

    # ---- inputs ----------------------------------------------------------------------------------

    def read_ipo_symbols(self) -> tuple[list[str], date]:
        metadata = pd.read_csv(self.raw_dir / 'ipo_metadata.csv', dtype=str)
        metadata = metadata[metadata['securitytype'].str.strip().isin(IPO_SECURITY_TYPES)]
        close_dates = pd.to_datetime(metadata['ipoenddate'].str.strip(), format='%d-%b-%Y', errors='coerce')
        return sorted(metadata['symbol'].str.strip().unique()), close_dates.min().date()

    # ---- daily files -----------------------------------------------------------------------------

    def cache_day(self, day: date):
        """Download one day's bhavcopy and index file into the cache (no-op if already cached)"""
        prices_path = self.cache_dir / 'bhavcopy' / f'{day.isoformat()}.parquet'
        index_path = self.cache_dir / 'indices' / f'{day.isoformat()}.csv'
        no_trading_path = self.cache_dir / 'no_trading' / f'{day.isoformat()}'
        if no_trading_path.exists() or (prices_path.exists() and index_path.exists()):
            return

        primary, fallback = ((udiff_bhavcopy_url, parse_udiff_bhavcopy), (cm_bhavcopy_url, parse_cm_bhavcopy))
        if day < UDIFF_START:
            primary, fallback = fallback, primary

        text, parser = None, None
        for url_for, parse in (primary, fallback):
            text = self.fetch(url_for(day), unzip=True)
            if text is not None:
                parser = parse
                break

        if text is None:
            # A missing file for a recent day may just not be published yet; only remember old ones
            if (datetime.now(IST).date() - day).days > 3:
                no_trading_path.parent.mkdir(parents=True, exist_ok=True)
                no_trading_path.touch()
            return

        prices_path.parent.mkdir(parents=True, exist_ok=True)
        parser(text).to_parquet(prices_path, index=False)

        index_text = self.fetch(index_close_url(day))
        if index_text is not None:
            index_path.parent.mkdir(parents=True, exist_ok=True)
            index_path.write_text(index_text, encoding='utf-8')

    def fetch(self, url: str, unzip: bool = False) -> str | None:
        """GET with retry and backoff; None when the file doesn't exist (holiday)"""
        session = getattr(self._local, 'session', None)
        if session is None:
            session = self._local.session = requests.Session()
            session.headers.update(HEADERS)

        for attempt in range(self.retries):
            try:
                response = session.get(url, timeout=30)
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                content = response.content
                if unzip:
                    with zipfile.ZipFile(io.BytesIO(content)) as archive:
                        content = archive.read(archive.namelist()[0])
                return content.decode('utf-8', errors='replace')
            except (requests.RequestException, zipfile.BadZipFile) as error:
                if attempt == self.retries - 1:
                    raise RuntimeError(f"Failed to fetch {url}: {error}") from error
                time.sleep(2 ** attempt)
        return None

    # ---- outputs ---------------------------------------------------------------------------------

    def linked_prices(self, ipo_symbols: list[str]) -> pd.DataFrame:
        """Rows for the IPO symbols, every security first traded after the start of the cache (an IPO can
        list under a symbol other than the one in the feed, e.g. DCX -> DCXINDIA), and everything reachable
        from those through a shared symbol, ISIN or ISIN issuer prefix.

        Following ISINs picks up renames (ZOMATO -> ETERNAL); following symbols picks up new ISINs issued on
        a face-value split; the issuer prefix (first 9 characters: country, issuer, equity) picks up a new
        symbol and ISIN together, as on some SME -> mainboard migrations. Over-inclusion is resolved in SQL.
        """
        con = duckdb.connect()
        glob = (self.cache_dir / 'bhavcopy' / '*.parquet').as_posix()
        con.execute(f"""
            CREATE TABLE prices AS
            SELECT * FROM read_parquet('{glob}')
            WHERE trade_dt <= DATE '{self.snapshot.isoformat()}'
        """)
        con.execute("CREATE TABLE symbols AS SELECT UNNEST(?::VARCHAR[]) AS symbol", [ipo_symbols])
        # A first trade within 30 days of an IPO's close, with a previous close equal to a price in its band
        con.execute(f"""
            CREATE TABLE ipo_bands AS
            WITH ipo AS (
                SELECT
                    TRY_STRPTIME(TRIM(ipoenddate), '%d-%b-%Y')::DATE AS ipo_end_dt,
                    list_transform(regexp_extract_all(pricerange || ' ' || COALESCE(issueprice, ''), '\\d+(?:\\.\\d+)?'),
                                   x -> TRY_CAST(x AS DOUBLE)) AS prices
                FROM read_csv('{(self.raw_dir / 'ipo_metadata.csv').as_posix()}', all_varchar=true)
                WHERE TRIM(securitytype) IN {IPO_SECURITY_TYPES}
            )
            SELECT ipo_end_dt, prices FROM ipo WHERE ipo_end_dt IS NOT NULL
        """)
        con.execute("""
            INSERT INTO symbols
            WITH first_trades AS (
                SELECT symbol, arg_min(trade_dt, trade_dt) AS first_dt, arg_min(prev_close, trade_dt) AS base_price
                FROM prices GROUP BY symbol
            )
            SELECT DISTINCT f.symbol
            FROM first_trades f
            JOIN ipo_bands b
                ON f.first_dt > b.ipo_end_dt AND f.first_dt <= b.ipo_end_dt + INTERVAL 30 DAY
               AND list_contains(b.prices, f.base_price)
            WHERE f.first_dt > (SELECT MIN(trade_dt) FROM prices) + INTERVAL 7 DAY
        """)

        size = -1
        while True:
            con.execute("""
                CREATE OR REPLACE TABLE isins AS
                SELECT DISTINCT isin FROM prices
                WHERE isin IS NOT NULL AND LEFT(isin, 9) IN (
                    SELECT DISTINCT LEFT(isin, 9) FROM prices WHERE symbol IN (SELECT symbol FROM symbols)
                )
            """)
            con.execute("""
                CREATE OR REPLACE TABLE symbols AS
                SELECT symbol FROM symbols
                UNION
                SELECT DISTINCT symbol FROM prices WHERE isin IN (SELECT isin FROM isins)
            """)
            new_size = con.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
            if new_size == size:
                break
            size = new_size

        df = con.execute(f"""
            SELECT {', '.join(SNAPSHOT_PRICE_COLUMNS)} FROM prices
            WHERE symbol IN (SELECT symbol FROM symbols)
            ORDER BY symbol, trade_dt, series
        """).df()
        con.close()
        df['trade_dt'] = pd.to_datetime(df['trade_dt']).dt.date
        return df

    def nifty50(self, days: list[date]) -> pd.DataFrame:
        rows = []
        for day in days:
            path = self.cache_dir / 'indices' / f'{day.isoformat()}.csv'
            if path.exists():
                row = parse_index_close(path.read_text(encoding='utf-8'))
                if row is not None:
                    rows.append(row)
        return pd.DataFrame(rows, columns=['trade_dt', 'open', 'high', 'low', 'close'])

    def corporate_actions(self, start: date) -> pd.DataFrame:
        session = requests.Session()
        session.headers.update({**HEADERS, 'Accept': 'application/json, text/plain, */*',
                                'Referer': CORPORATE_ACTIONS_PAGE})
        session.get(CORPORATE_ACTIONS_PAGE, timeout=30)  # sets the cookies the API requires

        frames = []
        for year in range(start.year, self.snapshot.year + 1):
            period_start = max(date(year, 1, 1), start)
            period_end = min(date(year, 12, 31), self.snapshot)
            for index in ('equities', 'sme'):
                params = {'index': index, 'from_date': f'{period_start:%d-%m-%Y}', 'to_date': f'{period_end:%d-%m-%Y}'}
                for attempt in range(self.retries):
                    try:
                        response = session.get(CORPORATE_ACTIONS_API, params=params, timeout=60)
                        response.raise_for_status()
                        records = response.json()
                        break
                    except (requests.RequestException, ValueError) as error:
                        if attempt == self.retries - 1:
                            raise RuntimeError(f"Corporate actions {index} {year} failed: {error}") from error
                        time.sleep(2 ** attempt)
                        session.get(CORPORATE_ACTIONS_PAGE, timeout=30)
                df = pd.DataFrame(records)
                if df.empty:
                    continue
                df['source_index'] = index
                frames.append(df)
                print(f"  corporate actions {index} {year}: {len(df)} rows")
                time.sleep(1)

        actions = pd.concat(frames, ignore_index=True)
        actions = actions[actions['subject'].fillna('').str.contains(CORPORATE_ACTION_PATTERN, regex=True)]
        columns = ['symbol', 'series', 'comp', 'isin', 'faceVal', 'exDate', 'recDate', 'subject', 'source_index']
        actions = actions[columns].rename(columns={
            'comp': 'company', 'faceVal': 'face_value', 'exDate': 'ex_date', 'recDate': 'record_date',
        })
        return actions.sort_values(['symbol', 'ex_date', 'source_index']).reset_index(drop=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Download NSE price history for a raw snapshot.")
    parser.add_argument('--snapshot', type=date.fromisoformat, required=True,
                        help="Snapshot date (YYYY-MM-DD); must already contain ipo_metadata.csv")
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args(argv)

    scraper = NSEPriceScraper(args.snapshot, workers=args.workers)
    scraper.save(scraper.scrape())


if __name__ == '__main__':
    main()
