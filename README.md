# IPO Analysis — Indian IPOs, 2016–2025

> **Status: work in progress.** The data warehouse, including listing-day prices and split-adjusted returns, is built and checked. The data overview, core analysis and model notebooks and a local Streamlit dashboard are done; deploying the dashboard and the final write-up are still to come. See [ROADMAP.md](ROADMAP.md) for the plan.

**Early findings** (snapshot 2026-02-15; details and confidence intervals in [notebook 02](notebooks/02_subscription_vs_returns.ipynb) and [notebook 03](notebooks/03_listing_gain_model.ipynb)):
- **Subscription strongly predicts the listing-day gain in both segments** (rank correlation 0.76 mainboard, 0.70 SME), and not just because of hot years. Mainboard IPOs subscribed 3.5× or less open at a median of −1%; those subscribed 79× or more open at +45%.
- **It says nothing about what happens after listing day.** Someone buying at the listing-day close typically trailed NIFTY over the next year (median −16 pp SME; −8 pp mainboard, which is borderline once the number of tests is accounted for), however big the pop or the subscription.
- **Who bids matters differently by segment.** For mainboard, QIB demand is a stronger signal than retail demand. For recent SME IPOs, QIB demand is the weakest of the three categories.
- **A small model adds little to subscription alone.** Trained on 2016–2023 and tested on 2024–25 listings, a logistic regression ranks which IPOs will open above issue price well (AUC 0.83 mainboard, 0.82 SME). For mainboard it is no better than the subscription multiple on its own; for SME, NII demand improves it slightly. Its probabilities came out too optimistic in 2024–25 (mean forecast 87% vs 78% actual for SME), because subscription multiples rose while a given multiple listed positive less often.

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
| With a subscription record | 450 | 660 | 1,110 |
| With subscription figures (the record has numbers) | 450 | 598 | 1,048 |
| With market data | 449 | 679 | 1,128 |
| With listing prices and subscription data | 448 | 660 | 1,108 |
| With a 1-year return (listed ≥ 1 year before the snapshot and still trading) | 356 | 573 | 929 |

**Listing day.** The listing is the IPO's first trade in the NSE bhavcopy within 30 days of issue close. On that day the bhavcopy's previous close is the exchange's base price, which equals the issue price derived from the NSE feed for 99.6% of listings — a check on both the listing day and the issue price. Where the two differ but the base price is one of the feed's prices for the issue, the base price is used (4 issues).

**Returns.** Every return is measured from the issue price and adjusted for bonuses and splits by following the security through renames and ISIN changes. Horizons (1m, 6m, 1y, 3y) are counted from the listing date; NIFTY 50 is measured over the same money-at-risk period (issue close → horizon). Adjusted returns match Yahoo Finance's split-adjusted prices on a sample of 18 IPOs with corporate actions.

**Matching.** IPOs are matched to subscription records on issue close date (±3 days) plus company-name similarity, and to Tickertape stocks mainly through their current NSE symbol. Cases the rules can't settle were reviewed by hand and recorded in `data/seeds/`.

## Notebooks

| Notebook | What it covers |
|---|---|
| [01_data_overview](notebooks/01_data_overview.ipynb) | Coverage and structural gaps, return distributions, base rates by year and segment, why `return_since_issue_pct` can't be compared across years, survivorship, and what the data can and can't support |
| [02_subscription_vs_returns](notebooks/02_subscription_vs_returns.ipynb) | Does subscription predict the listing gain? QIB vs retail vs NII demand, whether listing pops persist, and hot vs cold years. Rank correlations and median differences with bootstrap CIs, within-year checks, median regression and a verdict table |
| [03_listing_gain_model](notebooks/03_listing_gain_model.ipynb) | Will an IPO open above its issue price? One logistic regression per segment, trained on listings to 2023 and tested on 2024 onwards, against three baselines (base rate, "subscription > 10×", subscription alone). ROC, calibration, coefficients, a `> 10%` version and a simple backtest with the allotment caveat |

## Dashboard

A Streamlit app over the committed exports, with no scraping, DuckDB or model at runtime. Mainboard and SME are shown side by side in every view.

| Page | What it shows |
|---|---|
| Overview | Headline numbers by segment, IPOs and share opening above issue by listing year, the join funnel |
| Explore | Filter by segment, year, sector and issue size; subscription vs listing gain, and the median by subscription group |
| IPO lookup | One IPO's subscription by category, listing day and returns against NIFTY 50; link with `?ticker=SYMBOL` |
| Findings | The answers from notebooks 02 and 03 with their charts |

```bash
streamlit run app/streamlit_app.py
```

To deploy on Streamlit Community Cloud, use `app/streamlit_app.py` as the entrypoint and Python 3.11; it installs `app/requirements.txt`.

Notebooks read the latest mart export through `src/data.py` (set `IPO_SNAPSHOT=YYYY-MM-DD` to pick another) and save their charts to `reports/figures/`. Statistics helpers are in `src/stats.py`.

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

# Dashboard
streamlit run app/streamlit_app.py

# Tests (build the warehouse from small fixture files; the dashboard tests use the committed mart)
pytest

# Re-run a notebook and save its outputs
jupyter nbconvert --to notebook --execute --inplace notebooks/01_data_overview.ipynb
jupyter nbconvert --to notebook --execute --inplace notebooks/02_subscription_vs_returns.ipynb   # ~2 minutes (bootstraps)
jupyter nbconvert --to notebook --execute --inplace notebooks/03_listing_gain_model.ipynb        # ~1 minute
```

## Known issues

- Returns are price returns: dividends are not included (nor are they in the NIFTY 50 price index used as the benchmark).
- Demergers are not adjusted. A large one shows up as an unexplained price gap (QUESS, SHANKARA); returns measured across such a gap are left empty (`price_gap_dt`).
- SME listing gains are truncated in two periods. From 2024-07-04 NSE caps an SME IPO's opening price at 90% over the issue price (`listing_capped`, 43 IPOs). Before 26 Apr 2018 no SME listing opened above +20% (46 at exactly +20%); this is observed in the data, but no circular was found for it.
- CAMS, PROTEAN and NSDL listed on BSE only at IPO: CAMS and PROTEAN have no listing prices; NSDL is not in the mart.
- A horizon return is empty when the stock had no NSE trade in the 30 days before the horizon date. This affects 11 of 940 due 1-year returns and 20 of 494 due 3-year returns, all SME and mostly thinly traded 2016–2019 stocks.
- Chittorgarh has no SME subscription records for 2016 and lists most 2017 SME issues without numbers (`has_subscription_figures`), so SME subscription analysis effectively starts in 2018.
- Tickertape fields (market cap, P/E, ROE, P/B) describe each company at the snapshot date, not at listing.
- Chittorgarh uses `9999` as a placeholder for a missing subscription multiple (Salasar Techno's QIB figure); it is stored as unknown.
- SME QIB subscription figures exist mainly from 2023, so QIB comparisons for SME describe recent issues only.

## Disclaimer

For educational purposes only. Not investment advice.
