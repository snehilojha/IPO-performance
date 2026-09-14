# IPO Analysis — Indian IPOs, 2016–2025

> **Status: work in progress.** The data pipeline runs; the analysis has not been written yet. No findings are claimed here until they are backed by executed notebooks. See [ROADMAP.md](ROADMAP.md) for the plan.

## What this is

An analytics project on NSE-listed IPOs (SME and mainboard). It asks whether subscription demand at IPO — overall, and institutional (QIB) versus retail — tells you anything about listing-day gains and longer-run returns.

## Pipeline

```
scrapers (NSE, Chittorgarh, Tickertape)
   → data/raw/<snapshot-date>/*.csv
   → DuckDB raw tables
   → stg_* staging tables  (sql_transformations/)
   → mart_ipo_analytics
   → data/staged/mart_ipo_analytics_<snapshot>.csv
```

## Data sources

| Source | What it provides |
|---|---|
| NSE past issues | Symbol, company, issue dates, listing date, security type, issue price |
| Chittorgarh | Subscription by category (QIB, NII, retail, total) and issue size |
| Tickertape screener | Current price, market cap, sector, PE/ROE/PB |

Raw data is kept as dated snapshots in `data/raw/` (2026-01-22, 2026-01-30, 2026-02-15). A snapshot is never overwritten.

## Running it

Requires Python 3.11.

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows; use `source .venv/bin/activate` elsewhere
pip install -r requirements.txt

# Rebuild the warehouse from a saved snapshot (no scraping)
python pipeline.py --snapshot 2026-02-15 --skip-scrape

# Scrape a fresh snapshot dated today, then build
python pipeline.py
```

## Known issues

Being fixed in Phase 1 of the roadmap:

- `listing_gains_pct` in the mart is return from issue price to the **scrape-date** price, not the listing-day gain.
- The Tickertape join on ticker alone can attach the wrong company when tickers collide.
- Part of the subscription data uses a second layout whose dates are not parsed, so those IPOs drop out.
- Non-equity issues (REITs, InvITs) are not yet filtered out.

## Disclaimer

For educational purposes only. Not investment advice.
