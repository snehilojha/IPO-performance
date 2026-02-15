# import requests
# import time
# import pandas as pd


# # CONFIG


# URL = "https://api.tickertape.in/screener/query"

# HEADERS = {
#     "accept": "application/json, text/plain, */*",
#     "content-type": "application/json",
#     "user-agent": "Mozilla/5.0",
#     "origin": "https://www.tickertape.in",
#     "referer": "https://www.tickertape.in/",
# }

# PAGE_SIZE = 100   # safe batch size
# OFFSET = 0
# SLEEP_SEC = 0.4

# OUTPUT_PATH = "data/raw/tickertape_full_screener.csv"


# # MAIN SCRAPER


# def tickertape_scraper():

#     all_rows = []

#     offset = OFFSET

#     while True:

#         payload = {
#             "match": {},
#             "sortBy": "mrktCapf",
#             "sortOrder": -1,
#             "count": PAGE_SIZE,
#             "offset": offset,
#             "project": [
#                 "subindustry",
#                 "mrktCapf",
#                 "lastPrice",
#                 "apef",
#                 "4wpct",
#                 "pr1d",
#                 "roe",
#                 "pbr",
#             ],
#             "sids": [],
#         }

#         print(f"Fetching offset {offset}")

#         r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)
#         r.raise_for_status()

#         data = r.json()

#         rows = data["data"]["results"]

#         if not rows:
#             break

#         for row in rows:

#             stock = row.get("stock", {})
#             adv = stock.get("advancedRatios", {})
#             info = stock.get("info", {})

#             record = {
#                 "sid": row.get("sid"),
#                 "ticker": info.get("ticker"),
#                 "name": info.get("name"),
#                 "sector": info.get("sector"),
#                 "subindustry": adv.get("subindustry"),
#                 "market_cap": adv.get("mrktCapf"),
#                 "last_price": adv.get("lastPrice"),
#                 "pe": adv.get("apef"),
#                 "roe": adv.get("roe"),
#                 "pb": adv.get("pbr"),
#                 "4w_return_pct": adv.get("4wpct"),
#                 "1d_return_pct": adv.get("pr1d"),
#             }

#             all_rows.append(record)

#         offset += PAGE_SIZE
#         time.sleep(SLEEP_SEC)

#     print(f"\nCollected {len(all_rows)} stocks")

#     df = pd.DataFrame(all_rows)
#     df.to_csv(OUTPUT_PATH, index=False)

#     print(f"Saved to {OUTPUT_PATH}")

# # ENTRY POINT

# if __name__ == "__main__":
#     tickertape_scraper()



import requests
import time
import pandas as pd
from pathlib import Path


URL = "https://api.tickertape.in/screener/query"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "user-agent": "Mozilla/5.0",
    "origin": "https://www.tickertape.in",
    "referer": "https://www.tickertape.in/",
}

PAGE_SIZE = 100
SLEEP_SEC = 0.4

class TickertapeScraper:
    """Scrapes full stock screener data from Tickertape."""

    def __init__(self, page_size: int = PAGE_SIZE, sleep_sec: float = SLEEP_SEC):
        self.page_size = page_size
        self.sleep_sec = sleep_sec

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ---------------------------
    # Public pipeline interface
    # ---------------------------

    def scrape(self) -> pd.DataFrame:
        all_rows = []
        offset = 0

        while True:

            payload = {
                "match": {},
                "sortBy": "mrktCapf",
                "sortOrder": -1,
                "count": self.page_size,
                "offset": offset,
                "project": [
                    "subindustry",
                    "mrktCapf",
                    "lastPrice",
                    "apef",
                    "4wpct",
                    "pr1d",
                    "roe",
                    "pbr",
                ],
                "sids": [],
            }

            print(f"Fetching offset {offset}")

            r = self.session.post(URL, json=payload, timeout=30)
            r.raise_for_status()

            data = r.json()

            rows = data["data"]["results"]

            if not rows:
                break

            for row in rows:

                stock = row.get("stock", {})
                adv = stock.get("advancedRatios", {})
                info = stock.get("info", {})

                record = {
                    "sid": row.get("sid"),
                    "ticker": info.get("ticker"),
                    "name": info.get("name"),
                    "sector": info.get("sector"),
                    "subindustry": adv.get("subindustry"),
                    "market_cap": adv.get("mrktCapf"),
                    "last_price": adv.get("lastPrice"),
                    "pe": adv.get("apef"),
                    "roe": adv.get("roe"),
                    "pb": adv.get("pbr"),
                    "4w_return_pct": adv.get("4wpct"),
                    "1d_return_pct": adv.get("pr1d"),
                }

                all_rows.append(record)

            offset += self.page_size
            time.sleep(self.sleep_sec)

        print(f"\nCollected {len(all_rows)} stocks")

        return pd.DataFrame(all_rows)

    def save_to_csv(self, df: pd.DataFrame, output_path: Path):
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_path, index=False)

        print(f"Saved to {output_path}")


# ---------------------------
# Standalone run
# ---------------------------

if __name__ == "__main__":
    scraper = TickertapeScraper()

    df = scraper.scrape()

    scraper.save_to_csv(df, Path("data/raw/tickertape_full_screener.csv"))
