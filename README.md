# IPO Analysis — Indian IPOs, 2016–2025

> **Status: work in progress.** The data warehouse is built and checked; the analysis has not been written yet. No findings are claimed here until they are backed by executed notebooks. See [ROADMAP.md](ROADMAP.md) for the plan.

## What this is

An analytics project on NSE-listed IPOs (SME and mainboard). It asks whether subscription demand at IPO — overall, and institutional (QIB) versus retail — tells you anything about listing-day gains and longer-run returns.

## Pipeline

```
scrapers (NSE, Chittorgarh, Tickertape)
   → data/raw/<snapshot>/*.csv                 one folder per scrape, never overwritten
   → DuckDB raw_* tables + seed_* tables       data/seeds/ (reviewed manual matches and exclusions)
   → stg_*        clean each source            sql_transformations/stg_*.sql
   → int_*        match IPOs to subscription and market data
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
| Tickertape screener | Current price, market cap, sector, PE/ROE/PB |

Raw data is kept as dated snapshots in `data/raw/` (2026-01-22, 2026-01-30, 2026-02-15).

## Coverage (snapshot 2026-02-15)

| Step | Mainboard | SME | All |
|---|---:|---:|---:|
| Equity issues in the NSE feed | 467 | 703 | 1,170 |
| Listed IPOs (excl. FPOs, withdrawn, partly-paid, follow-ons) = mart rows | 450 | 682 | 1,132 |
| With subscription data | 448 | 651 | 1,099 |
| With market data | 442 | 645 | 1,087 |
| With both | 440 | 617 | 1,057 |

IPOs are matched to subscription records on issue close date (±3 days) plus company-name similarity, and to Tickertape stocks on ticker plus name similarity. Renames below the similarity threshold were reviewed by hand and recorded in `data/seeds/`.

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

- `return_since_issue_pct` is issue price → scrape-date price. It is **not** the listing-day gain, and it is not adjusted for splits or bonus issues. Listing-day prices and adjusted returns come in Phase 2.
- For 128 SME IPOs the NSE feed gives the mainboard migration date instead of the listing date; `listing_dt` is left empty for them (`listing_dt_flag` explains why).
- Tickertape lists only currently traded stocks, so delisted and merged companies have no market data (`has_market_data = false`).
- Chittorgarh has no subscription data for most 2016–17 SME issues.

## Disclaimer

For educational purposes only. Not investment advice.
