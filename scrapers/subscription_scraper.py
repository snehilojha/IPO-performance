# import requests
# import pandas as pd
# import time
# from pathlib import Path

# BASE_URL = (
#     "https://webnodejs.chittorgarh.com/cloud/report/data-read/"
#     "{report_id}/1/1/{start}/{end}/0/0"
# )

# HEADERS = {
#     "User-Agent": "Mozilla/5.0",
#     "Accept": "application/json, text/plain, */*",
#     "Origin": "https://www.chittorgarh.com",
#     "Referer": "https://www.chittorgarh.com/",
# }

# START_YEAR = 2010
# END_YEAR = 2026

# OUTPUT_PATH = Path("data/raw/ipo_subscription_all_2010_2026.csv")


# def fetch_year(year: int, report_id: int):

#     url = BASE_URL.format(
#         report_id=report_id,
#         start=year,
#         end=year,
#     )

#     params = {
#         "search": "",
#         "v": "06-17",
#     }

#     r = requests.get(url, headers=HEADERS, params=params, timeout=30)
#     r.raise_for_status()

#     data = r.json()

#     return data.get("reportTableData", [])


# def ipo_subscription_scraper():

#     all_rows = []

#     for year in range(START_YEAR, END_YEAR + 1):

#         print(f"\nFetching year {year}")

#         # MAINBOARD

#         main_rows = fetch_year(year, report_id=21)

#         print(f"  Mainboard: {len(main_rows)}")

#         for row in main_rows:
#             row["ipo_type"] = "MAINBOARD"
#             row["year"] = year
#             all_rows.append(row)

#         time.sleep(0.4)


#         # SME

#         sme_rows = fetch_year(year, report_id=22)

#         print(f"  SME: {len(sme_rows)}")

#         for row in sme_rows:
#             row["ipo_type"] = "SME"
#             row["year"] = year
#             all_rows.append(row)

#         time.sleep(0.6)

#     df = pd.DataFrame(all_rows)

#     OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
#     df.to_csv(OUTPUT_PATH, index=False)

#     print(f"\nSaved {len(df)} rows to {OUTPUT_PATH}")


# if __name__ == "__main__":
#     ipo_subscription_scraper()

import requests
import pandas as pd
import time
from pathlib import Path


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
END_YEAR = 2026


class IPOSubscriptionScraper:
    """Scrapes IPO subscription data from Chittorgarh."""

    def __init__(
        self,
        start_year: int = START_YEAR,
        end_year: int = END_YEAR,
        sleep_main: float = 0.4,
        sleep_sme: float = 0.6,
    ):
        self.start_year = start_year
        self.end_year = end_year
        self.sleep_main = sleep_main
        self.sleep_sme = sleep_sme

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ---------------------------
    # Public pipeline interface
    # ---------------------------

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

    # ---------------------------
    # Internal helpers
    # ---------------------------

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

        data = r.json()

        return data.get("reportTableData", [])


# ---------------------------
# Standalone run
# ---------------------------

if __name__ == "__main__":

    scraper = IPOSubscriptionScraper()

    df = scraper.scrape()

    scraper.save_to_csv(df, Path("data/raw/ipo_subscription.csv"))
