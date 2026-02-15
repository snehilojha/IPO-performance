# import requests
# import pandas as pd
# from pathlib import Path
# import time
# import re

# BASE_URL = "https://www.nseindia.com/api/public-past-issues"

# HEADERS = {
#     "User-Agent": "Mozilla/5.0",
#     "Accept": "application/json, text/plain, */*",
#     "Referer": "https://www.nseindia.com/market-data/all-upcoming-issues-ipo",
#     "Origin": "https://www.nseindia.com",
# }

# PARAMS = {
#     "from_date": "01-01-2010",
#     "to_date": "01-01-2026",
#     "security_type": "all",
# }

# OUTPUT_PATH = Path("data/raw/ipo_metadata.csv")

# def normalize_column_name(col: str) -> str:
#     col = col.lower().strip()

#     col = col.replace("₹", " inr ")
#     col = col.replace("%", " pct ")

#     col = re.sub(r"[()]", "", col)

#     col = re.sub(r"[^a-z0-9]+", "_", col)

#     col = re.sub(r"_+","_", col)
#     return col.strip("_")

# def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
#     df.columns = [normalize_column_name(c) for c in df.columns]

#     bad_cols = [
#         c for c in df.columns
#         if not re.match(r"^[a-z][a-z0-9_]*$", c)
#     ]

#     if bad_cols:
#         raise ValueError(f"Invalid column names after normalization: {bad_cols}")
    
#     return df 

# def ipo_data_scraper():

#     session = requests.Session()
#     session.headers.update(HEADERS)

#     print("Fetching NSE IPO history...")

#     r = session.get(BASE_URL, params=PARAMS, timeout=30)
#     r.raise_for_status()

#     data = r.json()

#     df = pd.DataFrame(data)

#     df = enforce_schema(df)

#     OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
#     df.to_csv(OUTPUT_PATH, index=False)

#     print(f"Saved {len(df)} rows to {OUTPUT_PATH}")


# if __name__ == "__main__":
#     ipo_data_scraper()


import requests
import pandas as pd
from pathlib import Path
import re


BASE_URL = "https://www.nseindia.com/api/public-past-issues"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/market-data/all-upcoming-issues-ipo",
    "Origin": "https://www.nseindia.com",
}

PARAMS = {
    "from_date": "01-01-2001",
    "to_date": "01-01-2026",
    "security_type": "all",
}

class IPOMetadataScraper:
    """Scrapes historical IPO metadata from NSE."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)


    # Public API for pipeline


    def scrape(self) -> pd.DataFrame:
        print("Fetching NSE IPO history...")

        r = self.session.get(BASE_URL, params=PARAMS, timeout=30)
        r.raise_for_status()

        data = r.json()

        df = pd.DataFrame(data)

        df = self._enforce_schema(df)

        return df

    def save_to_csv(self, df: pd.DataFrame, output_path: Path):
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_path, index=False)

        print(f"Saved {len(df)} rows to {output_path}")


    # Internal helpers


    def _normalize_column_name(self, col: str) -> str:
        col = col.lower().strip()

        col = col.replace("₹", " inr ")
        col = col.replace("%", " pct ")

        col = re.sub(r"[()]", "", col)
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = re.sub(r"_+", "_", col)

        return col.strip("_")

    def _enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [self._normalize_column_name(c) for c in df.columns]

        bad_cols = [
            c for c in df.columns
            if not re.match(r"^[a-z][a-z0-9_]*$", c)
        ]

        if bad_cols:
            raise ValueError(f"Invalid column names after normalization: {bad_cols}")

        return df

# Standalone run support


if __name__ == "__main__":
    scraper = IPOMetadataScraper()

    df = scraper.scrape()

    scraper.save_to_csv(df, Path("data/raw/ipo_metadata.csv"))



