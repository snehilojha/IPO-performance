"""
Weekly source check: are the four upstream sources still answering, and does the NSE feed still speak the
vocabulary data/seeds/security_types.csv records?

This does not take a snapshot. A full re-scrape needs the subscription source, which is down, and pulls
hundreds of megabytes of daily files; the point here is to notice an upstream change within a week of it
happening rather than months later, when a rebuild silently loses rows.

Exit 1 when a source that is expected to work fails, or when the feed returns a security type the seed does
not classify. Sources listed in KNOWN_BROKEN are reported but do not fail the run, so a long-standing
outage does not turn the weekly mail into noise.
"""

import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers import IST, equity_security_types
from scrapers.ipo_data_scraper import IPOMetadataScraper
from scrapers.ipo_tickertape_scraper import HEADERS as TICKERTAPE_HEADERS
from scrapers.ipo_tickertape_scraper import URL as TICKERTAPE_URL
from scrapers.nse_price_scraper import HEADERS as NSE_HEADERS
from scrapers.nse_price_scraper import udiff_bhavcopy_url
from scrapers.subscription_scraper import IPOSubscriptionScraper

# Down since at least 2026-09-14: the report API answers "No params data found" for every year.
KNOWN_BROKEN = {'Chittorgarh subscription'}

SEED = PROJECT_ROOT / 'data' / 'seeds' / 'security_types.csv'


def check_nse_past_issues() -> str:
    """The IPO list, and the security-type contract staging depends on."""
    feed = IPOMetadataScraper().scrape()
    if feed.empty:
        raise RuntimeError('the feed returned no rows')

    codes = feed['securitytype'].astype(str).str.strip().str.upper()
    classified = set(pd.read_csv(SEED)['security_type'].str.strip().str.upper())
    unknown = sorted(set(codes) - classified)
    if unknown:
        counts = ', '.join(f'`{code}` ({(codes == code).sum()} rows)' for code in unknown)
        raise RuntimeError(
            f'security types missing from data/seeds/security_types.csv: {counts}. '
            'Issues under them are being dropped from the mart. Classify each one in the seed.'
        )

    equity = codes.isin(equity_security_types())
    segments = pd.read_csv(SEED).set_index(
        pd.read_csv(SEED)['security_type'].str.strip().str.upper())['segment']
    mainboard = codes[equity].map(segments).eq('MAINBOARD').sum()
    sme = codes[equity].map(segments).eq('SME').sum()
    return f'{len(feed)} rows, {mainboard} mainboard and {sme} SME equity issues, every code classified'


def check_nse_bhavcopy() -> str:
    """One recent daily price file, the source every return is built from."""
    day = datetime.now(IST).date() - timedelta(days=1)
    for _ in range(7):
        if day.weekday() < 5:
            response = requests.get(udiff_bhavcopy_url(day), headers=NSE_HEADERS, timeout=60)
            if response.status_code == 200 and response.content:
                return f'{day.isoformat()} file served, {len(response.content) // 1024} KB'
        day -= timedelta(days=1)
    raise RuntimeError('no bhavcopy served for any of the last 7 weekdays')


def check_tickertape() -> str:
    """Sector, market cap and valuation at the snapshot date. One row is enough to prove the API answers."""
    payload = {'match': {}, 'sortBy': 'mrktCapf', 'sortOrder': -1, 'count': 1, 'offset': 0,
               'project': ['subindustry', 'mrktCapf', 'lastPrice'], 'sids': []}
    response = requests.post(TICKERTAPE_URL, json=payload, headers=TICKERTAPE_HEADERS, timeout=30)
    response.raise_for_status()
    results = response.json()['data']['results']
    if not results:
        raise RuntimeError('the screener returned no rows')
    return f"responding, top stock by market cap is {results[0].get('sid')}"


def check_chittorgarh() -> str:
    """Subscription by investor category: the project's independent variable."""
    year = datetime.now(IST).year
    rows = IPOSubscriptionScraper(start_year=year, end_year=year).scrape()
    if len(rows) == 0:
        raise RuntimeError(f'no subscription records for {year}; a fresh snapshot cannot be built')
    return f'{len(rows)} records for {year}'


CHECKS = {
    'NSE past issues': check_nse_past_issues,
    'NSE bhavcopy': check_nse_bhavcopy,
    'Tickertape screener': check_tickertape,
    'Chittorgarh subscription': check_chittorgarh,
}


def main() -> int:
    lines = [f'## Source check — {datetime.now(IST).date().isoformat()}', '', '| Source | Status | Detail |', '|---|---|---|']
    failures = []

    for name, check in CHECKS.items():
        try:
            detail = check()
            status = 'OK'
            if name in KNOWN_BROKEN:
                status = 'recovered'
                failures.append(f'{name} is answering again — take it out of KNOWN_BROKEN')
        except Exception as error:  # noqa: BLE001 - a probe reports any failure, it never raises
            detail = f'{type(error).__name__}: {error}'
            if name in KNOWN_BROKEN:
                status = 'down (known)'
            else:
                status = 'FAILED'
                failures.append(f'{name}: {detail}')
            print(f'--- {name} ---\n{traceback.format_exc()}', file=sys.stderr)

        lines.append(f'| {name} | {status} | {detail.replace("|", "/")} |')

    if failures:
        lines += ['', '### Needs attention', *(f'- {failure}' for failure in failures)]
    else:
        lines += ['', 'Every source behaved as expected.']

    report = '\n'.join(lines)
    print(report)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        Path(summary).write_text(report + '\n', encoding='utf-8')

    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
