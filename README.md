# IPO Analysis — Indian IPOs, 2016–2025

> **Status: work in progress.** The data warehouse — including listing-day prices and split-adjusted returns — is built and checked; the analysis has not been written yet. No findings are claimed here until they are backed by executed notebooks. See [ROADMAP.md](ROADMAP.md) for the plan.

## What this is

An analytics project on NSE-listed IPOs (SME and mainboard). It asks whether subscription demand at IPO — overall, and institutional (QIB) versus retail — tells you anything about listing-day gains and longer-run returns.

## Pipeline

```
scrapers (NSE past issues, Chittorgarh, Tickertape, NSE bhavcopy + index + corporate actions)
   → data/raw/<snapshot>/                      one folder per scrape, never overwritten
   → DuckDB raw_* tables + seed_* tables       data/seeds/ (reviewed matches, listings, exclusions, actions)
   → stg_*        clean each source            sql_transformations/stg_*.sql
   → int_*        listing day, price history (renames, splits, bonuses), matches, horizon returns
   → mart_ipo_analytics + audit_join_funnel
   → checks       sql_checks/*.sql — any violation stops the run
   → data/mart/mart_ipo_analytics_<snapshot>.parquet / .csv
     reports/join_funnel_<snapshot>.csv
```

## Data sources

| Source | What it provides |
|---|---|
| NSE past issues | Symbol, company, issue dates, listing date, security type, price band, issue price |
| Chittorgarh | Subscription by category (QIB, NII, retail, total), applications, issue size |
| Tickertape screener | Current market cap, sector, PE/ROE/PB |
| NSE CM bhavcopy (daily) | Unadjusted open/close and previous close of every NSE security, SME included — listing-day prices and price history |
| NSE index closing file (daily) | NIFTY 50 benchmark |
| NSE corporate actions | Bonus issues, splits and consolidations, used to adjust prices |

Raw data is kept as dated snapshots in `data/raw/` (2026-01-22, 2026-01-30, 2026-02-15). Only 2026-02-15 carries the price files; add them to another snapshot with `python -m scrapers.nse_price_scraper --snapshot <date>`. Downloaded NSE daily files are cached in `data/cache/nse/` (not committed).

## Coverage (snapshot 2026-02-15)

| Step | Mainboard | SME | All |
|---|---:|---:|---:|
| Equity issues in the NSE feed | 467 | 703 | 1,170 |
| IPOs (excl. FPOs, withdrawn, partly-paid, follow-ons) | 457 | 697 | 1,154 |
| Listed on NSE = mart rows | 452 | 691 | 1,143 |
| With listing-day prices | 450 | 691 | 1,141 |
| With subscription data | 450 | 660 | 1,110 |
| With market data | 449 | 679 | 1,128 |
| With listing prices and subscription data | 448 | 660 | 1,108 |
| With a 1-year return (listed ≥ 1 year before the snapshot and still trading) | 356 | 573 | 929 |

**Listing day.** The listing is the IPO's first trade in the NSE bhavcopy within 30 days of issue close. On that day the bhavcopy's previous close is the exchange's base price, which equals the issue price derived from the NSE feed for 99.6% of listings — a check on both the listing day and the issue price. Where the two differ but the base price is one of the feed's prices for the issue, the base price is used (4 issues).

**Returns.** Every return is measured from the issue price and adjusted for bonuses and splits by following the security through renames and ISIN changes. Horizons (1m, 6m, 1y, 3y) are counted from the listing date; NIFTY 50 is measured over the same money-at-risk period (issue close → horizon). Adjusted returns match Yahoo Finance's split-adjusted prices on a sample of 18 IPOs with corporate actions.

**Matching.** IPOs are matched to subscription records on issue close date (±3 days) plus company-name similarity, and to Tickertape stocks mainly through their current NSE symbol. Cases the rules can't settle were reviewed by hand and recorded in `data/seeds/`.

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

# Tests (build the warehouse from small fixture files)
pytest
```

## Known issues

- Returns are price returns: dividends are not included (nor are they in the NIFTY 50 price index used as the benchmark).
- Demergers are not adjusted. A large one shows up as an unexplained price gap (QUESS, SHANKARA); returns measured across such a gap are left empty (`price_gap_dt`).
- From 2024-07-04 NSE caps an SME IPO's opening price at 90% over the issue price, so later SME listing gains are truncated (`listing_capped`).
- CAMS, PROTEAN and NSDL listed on BSE only at IPO: CAMS and PROTEAN have no listing prices; NSDL is not in the mart.
- Horizon returns are empty when the stock stopped trading on NSE before the horizon (delisted, suspended or moved), which biases long-horizon averages towards survivors.
- Chittorgarh has no subscription data for most 2016–17 SME issues.

## Disclaimer

For educational purposes only. Not investment advice.
