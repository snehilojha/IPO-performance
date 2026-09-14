"""Chittorgarh subscription reports: subscription by category, applications and issue size, mainboard and SME."""

import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from scrapers import IST

BASE_URL = (
    "https://webnodejs.chittorgarh.com/cloud/report/data-read/"
    "{report_id}/1/1/{start}/{end}/0/0"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.chittorgarh.com",
    "Referer": "https://www.chittorgarh.com/",
}

START_YEAR = 2001


class IPOSubscriptionScraper:
    """Scrapes IPO subscription data from Chittorgarh."""

    def __init__(
        self,
        start_year: int = START_YEAR,
        end_year: int | None = None,
        sleep_main: float = 0.4,
        sleep_sme: float = 0.6,
    ):
        self.start_year = start_year
        self.end_year = end_year or datetime.now(IST).year
        self.sleep_main = sleep_main
        self.sleep_sme = sleep_sme

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # Public pipeline interface

    def scrape(self) -> pd.DataFrame:
        all_rows = []

        for year in range(self.start_year, self.end_year + 1):
            print(f"\nFetching year {year}")

            # MAINBOARD
            main_rows = self._fetch_year(year, report_id=21)
            print(f"  Mainboard: {len(main_rows)}")

            for row in main_rows:
                row["ipo_type"] = "MAINBOARD"
                row["year"] = year
                all_rows.append(row)

            time.sleep(self.sleep_main)

            # SME
            sme_rows = self._fetch_year(year, report_id=22)
            print(f"  SME: {len(sme_rows)}")

            for row in sme_rows:
                row["ipo_type"] = "SME"
                row["year"] = year
                all_rows.append(row)

            time.sleep(self.sleep_sme)

        return pd.DataFrame(all_rows)

    def save_to_csv(self, df: pd.DataFrame, output_path: Path):
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_path, index=False)

        print(f"\nSaved {len(df)} rows to {output_path}")

    # Internal helpers

    def _fetch_year(self, year: int, report_id: int):
        url = BASE_URL.format(
            report_id=report_id,
            start=year,
            end=year,
        )

        params = {
            "search": "",
            "v": "06-17",
        }

        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()

        return r.json().get("reportTableData", [])


if __name__ == "__main__":
    scraper = IPOSubscriptionScraper()
    df = scraper.scrape()
    scraper.save_to_csv(df, Path("data/raw/ipo_subscription.csv"))
