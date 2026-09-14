# IPO Analysis — Roadmap

The single source of truth for finishing this project. Work top to bottom, tick boxes as you go, and commit at the end of each phase. When Phase 7 is done, delete this file or move it to `docs/`.

**End state:** a trustworthy warehouse → a clearly stated question answered with the right statistics → one small, honest model → a live Streamlit dashboard → a README that leads with the finding.

**Scope decisions (fixed):**
- SME and mainboard IPOs are both analysed, **segmented in every cut, never pooled**.
- The ML scaffolding is removed. The only model is one leakage-free logistic regression (Phase 5).
- Dashboard: Streamlit.

---

## Why this roadmap exists — verified problems

Checked against the pre-Phase 1 export `data/staged/mart_ipo_analytics_20260215.csv` (snapshot 2026-02-15, since removed) and `data/duckdb/ipo.duckdb`.

| # | Problem | Evidence | Fixed in |
|---|---|---|---|
| 1 | `listing_gains_pct` is not listing gains — it's return from issue price to **scrape-date** price | Matches `(current_price − issue_price)/issue_price` on 100% of rows; max +4,260% | Phase 1, 2 |
| 2 | Holding period isn't held constant | Median hold: ≤2023 cohort ~1,245 days, 2024 ~563, 2025 ~267 | Phase 1, 2 |
| 3 | Tickertape join on `ticker` alone attaches the wrong company | GSTL → Globesecure *and* Globalspace; KEL, MAL, ZEAL, RAJPUTANA, SEL duplicated with different prices/sectors | Phase 1 |
| 4 | 110 subscription rows can never join | Second Chittorgarh layout (`Company Name`, `Closing Date`/`Close Date`) — dates not parsed, stay NULL | Phase 1 |
| 5 | Exact name + date join loses ~40% of IPOs | 1,141 listed equity IPOs → 690 in mart. Fuzzy match (close date ±3d, Jaro-Winkler ≥0.90) recovers 1,101 | Phase 1 |
| 6 | Non-equity issues mixed in | NSE feed includes DEBT, N*/Z*/Y* NCDs, RR (REITs), IV (InvITs), W1 (warrants) | Phase 1 |
| 7 | `COALESCE(pe/roe/pb, 0)` turns unknowns into zeros | `stg_tickertape_data_table.sql` | Phase 1 |
| 8 | Survivorship: mart `WHERE tt.ticker IS NOT NULL` drops delisted/renamed companies silently | ~58 listed equity IPOs have no Tickertape match | Phase 1, 7 |
| 9 | QIB missingness is structural | QIB null for 49% of SME, 0% of mainboard | Phase 1, 4 |
| 10 | ML layer never run and leaks | Notebooks have 0 executed cells; `engineer_features()` target-encodes on full data before the split; notebook 04 scores unscaled data | Phase 0, 5 |
| 11 | Docs make false claims | README: "728 IPOs, 2001–2026", `[X]%` placeholders; real NSE feed starts mid-2016 | Phase 0, 7 |
| 12 | Environment broken | `.venv` points to `C:\Users\Admin\...\Python311`; requirements unpinned | Phase 0 |

---

## Phase 0 — Reset repo and environment

- [x] Commit the current untracked ML scaffolding to branch `archive/ml-scaffold` (safety copy — it is untracked, so deletion is otherwise irreversible). Return to `main`.
- [x] Delete from `main`: `src/` (models, evaluation, features, config), `notebooks/01–04_*.ipynb`, `notebooks/modeling.ipynb`, `reports/project_report.md`. `src/config.py` is recreated in Phase 3.
- [x] Replace `README.md` with a short, honest work-in-progress stub (what it is, current status, link to this roadmap). No results, no placeholders.
- [x] Rebuild `.venv` on Python 3.11.
- [x] Pin `requirements.txt` to exact versions (direct dependencies only). Removed `xgboost`, `lightgbm`, `shap`, `imbalanced-learn`, `seaborn`, `beautifulsoup4` (unused). Added `yfinance`, `scipy`, `statsmodels`, `plotly`, `streamlit`, `pyarrow`, `pytest`, `ruff`.
- [x] Raw data convention: one folder per scrape — `data/raw/2026-01-22/`, `data/raw/2026-01-30/`, `data/raw/2026-02-15/`. Scraping refuses to overwrite an existing snapshot or to write into a past date.
- [x] `pipeline.py`:
  - [x] CLI flags `--snapshot YYYY-MM-DD` and `--skip-scrape` (rebuild from saved raw files without hitting any site).
  - [x] Move `setup_logging()` out of import time into `main`.
  - [x] Open SQL files (and the log file) with `encoding="utf-8"`.
  - [x] Mart export is named after the snapshot date, not the run date.
- [x] `.gitignore`: add `models/` (`data/duckdb/` was already covered by `duckdb/` and `*.duckdb`).
- [x] Commits per step.

**Done when:** a fresh venv installs cleanly and `python pipeline.py --snapshot 2026-02-15 --skip-scrape` reproduces the current mart.

> ✅ **Verified 2026-09-14:** rebuilding 2026-02-15 reproduces `mart_ipo_analytics_20260215.csv` exactly (690 rows, identical apart from `etl_processed_timestamp`). Rebuilding 2026-01-30 gives 729 rows vs the original 728 — the extra IPO (OMFURN) is recovered by the deterministic subscription dedupe fix.
> Carried into Phase 7: `ruff` currently reports style findings in `pipeline.py` (f-string `str(e)`, `logger.error(exc_info=True)`, naive datetimes); pick the rule set when adding CI.

---

## Phase 1 — Fix the warehouse (SQL, matching, checks)

Files: `sql_transformations/` (`00_macros`, `stg_*`, `int_*`, `mart_ipo_analytics`, `audit_join_funnel`), `pipeline.py`, `sql_checks/`, `data/seeds/`, `tests/`.

### 1.1 `stg_past_ipo`
- [x] Filter `securitytype IN ('EQ', 'BE', 'SME')`.
- [x] `issue_price`: the feed's `issueprice` **only when it lies inside the price band**, otherwise the band's upper end. *Changed from plan:* `issueprice` falls outside the band for ~140 issues (e.g. NPST 2,177 against a Rs 76–80 band — a post-migration price). `issue_price_source` records which was used.
- [x] Add `segment` (`SME` if security type is SME, else `MAINBOARD`).
- [x] Add `company_name_norm` (macro `norm_company_name` in `00_macros.sql`).
- [x] `listing_dt_flag`. *Found:* for 128 SME IPOs NSE reports the **SME → mainboard migration date** as the listing date (1–8 years after issue); 2 mainboard rows (CAMS, Protean) show a similar later date. `listing_dt` is NULL for these; `listing_dt_reported` keeps the raw value.
- [x] *Added:* `exclusion_reason` — FPOs and withdrawn issues (by name), partly-paid / demerger listings (`data/seeds/excluded_issues.csv`), and follow-on issues (a later issue under an already-used symbol).
- [x] Drop the `_1` suffix; the DuckDB file is now recreated on every run, so stale tables can't survive.

### 1.2 `stg_ipo_subscription`
- [x] `company_name = COALESCE(Company, "Company Name")`.
- [x] `ipo_end_dt = COALESCE("~Issue_Close_Date", STRPTIME(COALESCE("Closing Date", "Close Date")))`, same for `ipo_start_dt`. *Found:* `~Issue_Close_Date` is populated for both layouts in all three snapshots — the rows were lost because the old join also required exact names **and** equal open dates (open date is NULL for 110 issues).
- [x] Keep `sNII (x)`, `bNII (x)`, `Employee (x)`, `Applications` (parsed from `2,11,219` style).
- [x] Add `company_name_norm` and `subscription_key`.
- [x] Dedupe on `(segment, company_name_norm, ipo_end_dt)` with a deterministic `ORDER BY`.

### 1.3 `stg_tickertape`
- [x] Keep `tickertape_sid` and name.
- [x] Ratios stay NULL when missing.
- [x] `is_lossmaking = (pe < 0 OR roe < 0)`; NULL when both unknown.
- [x] Add `company_name_norm`.

### 1.4 `int_ipo_subscription_match`
- [x] Candidates: close dates within ±3 days and Jaro-Winkler ≥ 0.90 on normalised names; same segment ranks first.
- [x] One-to-one: best candidate per IPO **and** per subscription record.
- [x] `match_score`, `close_date_diff_days`, `match_method` (`exact` / `fuzzy` / `manual`).
- [x] `data/seeds/manual_subscription_matches.csv` (L&T Infotech, Angel Broking). The 33 IPOs still unmatched are almost all 2016–17 SME issues that Chittorgarh doesn't cover.

### 1.5 `int_ipo_market_match`
- [x] Same-ticker candidates ranked by name similarity (resolves GSTL, KEL, MAL, ZEAL, RAJPUTANA, SEL, FOCUS, BRIGHT); accepted at ≥ 0.90.
- [x] *Added:* identical normalised name under a different ticker (`name` method) — recovers 11 changed symbols such as WORTH → WORTHPERI.
- [x] `data/seeds/manual_ticker_matches.csv`: 22 reviewed renames accepted (Adani Wilmar → AWL, Zomato → Eternal, LTI → LTIMindtree, …); 13 low-similarity pairs rejected, 9 of them marked *unverified* for Phase 2.
- [x] Unmatched IPOs are kept with `has_market_data = false` (45; mostly delisted or merged, e.g. Equitas Holdings, Ujjivan Financial).

### 1.6 Rebuild `mart_ipo_analytics`
- [x] Base = all listed IPOs; `LEFT JOIN` both matches. No silent drops (enforced by a check).
- [x] Flags: `has_subscription_data`, `has_market_data`, `has_qib_category`.
- [x] `listing_gains_pct` → `return_since_issue_pct`; `snapshot_date` from the CLI snapshot.
- [x] `holding_days` (from **issue close** — when the issue price is paid, and available even when the listing date isn't), `holding_years`, `annualized_return_pct` (NULL under 182 days).
- [x] Export `data/mart/mart_ipo_analytics_<snapshot>.parquet` and `.csv` (dated like the raw snapshots). The old `data/staged/` exports were removed.

### 1.7 Join funnel
- [x] `audit_join_funnel`, by segment: raw → equity → IPOs → listed → mart → with subscription → with market → with both. Logged each run; exported to `reports/join_funnel_<snapshot>.csv`.

### 1.8 Data-quality checks (pipeline stage 5 — fail loudly)
- [x] 11 checks in `sql_checks/`; any returned row raises `DataQualityError` before anything is exported. Thresholds live in `THRESHOLDS` in `pipeline.py`.
  - `(ticker, ipo_end_dt)` and `(ticker, listing_dt)` unique
  - `issue_price > 0`; `listing_dt >= ipo_end_dt`
  - every listed IPO is in the mart
  - automatic matches above threshold; matches one-to-one
  - mart rows ≥ 1,000; subscription and market coverage ≥ 90% **per segment**
  - no mainboard issue with a missing QIB figure

### 1.9 Tests
- [x] `tests/test_warehouse.py` — 16 tests building stages 2–5 from `tests/fixtures/`: ticker collision, second subscription layout, non-equity / FPO / withdrawn / partly-paid / follow-on exclusion, no duplicates, migration listing date, issue-price rule, best fuzzy candidate, ±days match, manual / name / rejected market matches, NULL ratios, returns, a failing check stops the export, missing snapshot. Mutation-checked: breaking the ranking or the migration rule makes the suite fail.

**Done when:** pipeline passes all checks, mart has ~1,050–1,100 rows, funnel is published, `pytest` is green.

> ✅ **Verified 2026-09-14** (snapshot 2026-02-15): all 11 checks pass; mart **1,132** IPOs (450 mainboard, 682 SME); subscription data **1,099 (97%)**, market data **1,087 (96%)**, both 1,057. QIB category present for 100% of mainboard and 48% of SME issues. `pytest`: 16 passed. Snapshots 2026-01-22 and 2026-01-30 also build and pass (1,145 and 1,144 rows — the earlier NSE scrapes contained more rows).
> Carried forward: `return_since_issue_pct` is not adjusted for splits/bonuses (Phase 2); `ruff` import-order and datetime findings (Phase 7).

---

## Phase 2 — Listing-day prices and NIFTY benchmark

Files: `scrapers/nse_price_scraper.py`, `sql_transformations/` (`stg_nse_prices`, `stg_nifty50`, `stg_corporate_actions`, `int_ipo_listing`, `int_ipo_price_history`, `int_ipo_horizon_returns`), `sql_checks/prices_*`, `data/seeds/manual_listings.csv`, `data/seeds/manual_corporate_actions.csv`, `tests/`.

- [x] **Coverage spike first.** 50 random IPOs (25 SME, 25 mainboard) on `yfinance`: data for 96% of mainboard but **64% of SME**, and Yahoo's first trading day equals the listing day for only 36% of SME. Yahoo's "unadjusted" open is also split-adjusted (SALASAR shows 2.50 against a 108 issue price). → *Changed from plan:* NSE archives are the only price source; `yfinance` is used only to cross-check.
- [x] `scrapers/nse_price_scraper.py`: the NSE CM bhavcopy for every trading day since the first issue close (old format to 2024-07-05, UDiFF after; SME series included), the daily index file (NIFTY 50), and the NSE corporate-actions API (bonus / split / consolidation). Per-day cache in `data/cache/nse/` (gitignored, ~320 MB), 4 threads, retry with backoff; holidays are remembered. Output to `data/raw/<snapshot>/`: `nse_prices.parquet` (9.6 MB, the ~1,300 securities linked to IPO symbols through shared symbols, ISINs or ISIN issuer prefixes, plus new listings at an IPO's issue price), `nifty50.csv`, `corporate_actions.csv`.
- [x] ~~Fallback to bhavcopy if SME coverage < 85%~~ — bhavcopy is the primary source for everything.
- [x] **SME listing cap verified:** NSE caps an SME IPO's opening price at 90% over the issue price from **2024-07-04** ([Business Standard, 4 Jul 2024](https://www.business-standard.com/markets/ipo/sme-ipo-opening-price-to-be-capped-at-90-in-pre-open-on-listing-day-nse-124070400429_1.html)). The data agrees: SME listings opened up to 4.9× the issue price before that date and never above 1.900× after it. `listing_cap_applies`, `listing_capped` (43 SME IPOs capped).
- [x] `stg_nse_prices`, `stg_nifty50`, `stg_corporate_actions` (ratios parsed from the subject text; bonus preference shares and debentures ignored).
- [x] `int_ipo_listing`: first NSE trade within 30 days of issue close — under the feed's symbol (1,131), or *added:* another symbol's first trade at a base price equal to the issue price (10, e.g. DCX → DCXINDIA, Sterling & Wilson → SWSOLAR), or a reviewed seed. On listing day the bhavcopy's previous close is the exchange base price; it equals the feed-derived issue price for 1,136 of 1,141 listings (99.6%).
- [x] *Added:* issue price reconciled with the listing base price when that price is one the feed gives for the issue (4 SME issues: WFL, RPPL, SUPREMEENG, ICEMAKE). The one remaining difference is AUBANK (base price 525 = its listing price, issue price 358).
- [x] `int_ipo_price_history`: each IPO's security followed from its listing row as a chain of (symbol, ISIN) periods — same ISIN (renames), same symbol or ISIN issuer prefix within 30 days (splits, SME → mainboard migrations). 862k daily rows.
- [x] **Split and bonus adjustment:** 325 factors applied from NSE corporate actions. *Found:* NSE files past actions under the company's **current** symbol (KARDA's 2021 split is listed under DHARAN), so actions are matched on any symbol of the chain. Two actions missing from the API were added from `data/seeds/manual_corporate_actions.csv` after checking the record dates (JSLL split, GICL bonus + split). Every factor with a trade on its ex-date agrees with the actual price move (270 of 270). Cross-checked against Yahoo split-adjusted closes for 18 IPOs with corporate actions: identical to 3 decimals.
- [x] *Added:* `int_ipo_price_gaps` — overnight moves no corporate action explains (share-adjusted open < 60% or > 167% of the previous close). 6 remain: two demergers (QUESS, SHANKARA) and four illiquid SME stocks. Returns measured across a gap are NULL; `price_gap_dt` flags them.
- [x] Mart columns: `listing_dt` (now from the bhavcopy), `listing_dt_source`, `listing_method`, `listing_symbol`, `listing_base_price`, `listing_open`, `listing_close`, `listing_volume`, `listing_gain_pct` (open vs issue price), `listing_day_close_gain_pct`, `return_{1m,6m,1y,3y}_pct` (issue price → adjusted close at listing + horizon), `nifty_return_*` (issue close → horizon), `excess_return_*`, `aftermarket_excess_return_*` (from the listing-day close), `last_trade_dt`, `is_trading_at_snapshot`, `share_multiplier`, `corporate_action_count`, `price_gap_dt`. `return_since_issue_pct` is now split/bonus-adjusted and measured to the last NSE trade. A horizon return is NULL if the horizon is after the snapshot, or the stock hadn't traded for 30 days before it (11 IPOs at 1y, 17 at 3y).
- [x] **Recovered real listing dates:** 129 IPOs whose feed listing date was a later event (128, mostly SME migrations) or before issue close (1), plus the 11 below with no feed listing date.
- [x] **Re-checked the 9 unverified rejected renames:** all 9 are the IPO's own security (the Tickertape ticker is the latest symbol of its ISIN chain), so the rejects were wrong and were removed. *Added:* market match method `nse_symbol` — the Tickertape ticker is the IPO's current NSE symbol. Market data 1,095 → 1,128; every IPO still trading on NSE now has it. No existing match changed.
- [x] **IPOs with no listing date in the feed:** 11 recovered from prices (9 SME, DCX, Sterling & Wilson). NSDL and CAMS listed on BSE only at IPO; CAMS's later NSE start is rejected in `data/seeds/manual_listings.csv`; PROTEAN stays without prices.
- [x] 10 checks in `sql_checks/prices_*`: listing price coverage per segment (≥ 85% mainboard, ≥ 75% SME), base price = issue price (≥ 98%), bhavcopy listing date = feed date where the feed was valid, listing open/close within 0.2–10× issue price, SME cap respected, corporate-action factors match price moves, one price row per day, horizon returns only when valid, ≤ 15 unexplained gaps, NIFTY present for every trading day.
- [x] Price coverage in the join funnel (listing price, 1-year return, listing price and subscription).
- [x] Tests: 34 (`test_warehouse.py` fixtures now include prices, NIFTY and corporate actions; `test_nse_price_scraper.py` covers both bhavcopy formats, the index file and security linking). Mutation-checked: removing chain-wide corporate-action matching, new-listing detection, ISIN-prefix linking, the price-gap guard, the SME cap or the duplicate-row guard each fails the suite.

**Done when:** true listing gain and fixed-horizon excess returns are in the mart with coverage reported.

> ✅ **Verified 2026-09-14** (snapshot 2026-02-15): all 21 checks pass. Mart **1,143** IPOs (452 mainboard, 691 SME). Listing prices **1,141 (99.8%)** — 450 mainboard, 691 SME. 1-year return 929 of the 940 IPOs listed at least a year before the snapshot; 3-year 474 of 494. Market data 1,128, subscription 1,110, listing price and subscription 1,108. Phase 1 still holds: the pipeline was re-run clean before starting, a fresh venv installs with `pip check` clean, and snapshot 2026-01-30 rebuilds and passes all checks once its price files are fetched (its feed has exact duplicate rows, which exposed a join bug now fixed and tested).
> Older snapshots (2026-01-22, 2026-01-30) don't carry price files in the repo; `python -m scrapers.nse_price_scraper --snapshot <date>` adds them from the cache.
> Carried forward: returns exclude dividends (as does the NIFTY 50 price index); demergers smaller than the gap threshold are not adjusted; `ruff` datetime and import-order findings in the new scraper (Phase 7).

---

## Phase 3 — Data overview notebook (`notebooks/01_data_overview.ipynb`)

- [x] Shared helpers:
  - `src/config.py` — paths, `available_snapshots()`, `resolve_snapshot()` (argument → `$IPO_SNAPSHOT` → latest export)
  - `src/data.py` — `load_mart()` converts DuckDB's DATE / DECIMAL / nullable BOOLEAN columns to datetime, float and nullable boolean, and adds `listing_year`; `load_funnel()`; `horizon_elapsed()` (same calendar-month rule as the SQL)
  - `src/plotting.py` — one chart style, segment colours, `save_fig()` → `reports/figures/`, `asinh_bins()`
- [x] *Found before starting — warehouse fix:* 62 SME IPOs (almost all 2017) match a Chittorgarh record that has no subscription numbers, so they showed `has_subscription_data = true` and `has_qib_category = false`. *Added:* `has_subscription_figures` (1,048 IPOs: 450 mainboard, 598 SME), `has_qib_category` is NULL when there are no figures, funnel step 12. Test added and seen failing first; the rebuilt mart differs from the previous one only in those two columns.
- [x] Coverage: join funnel, IPO count by year × segment, availability of every analysis field by segment (horizon returns as a share of those due), and a year × segment table showing the structural gaps: SME QIB category below 17% of issues in every year to 2022, then 46% / 76% / 90% in 2023–2025; application counts only from 2023 in both segments.
- [x] Distributions of `listing_gain_pct` and `excess_return_1y_pct` on an asinh axis, median marked, one panel per segment.
- [x] Base rates by listing year and segment: share opening above issue, median and IQR listing gain, median 1-year excess return (only for fully elapsed cohorts); cohorts under 20 IPOs marked.
- [x] Chart showing why `return_since_issue_pct` can't be compared across years: median return since issue, median holding period and median fixed 1-year return by listing year. Spearman ρ(holding days, return since issue) = 0.34 / 0.36; with the 1-year return, 0.04 / −0.04.
- [x] *Added:* survivorship section — which horizon returns are missing and why, and the returns recorded by the 34 stocks that stopped trading.
- [x] Closing markdown: what this data can and can't support.
- [x] *Found — second SME listing ceiling:* no SME listing opened above +20% before 26 Apr 2018 (134 listings, 46 at exactly +20%), consistent with the 20% daily price band applying on listing day. No circular found, so it is described as an observed ceiling, not a documented rule. Together with the +90% cap since July 2024, 89 SME listing gains sit exactly at a ceiling.

**Done when:** notebook runs top to bottom, outputs saved in the file.

> ✅ **Verified 2026-09-14** (snapshot 2026-02-15): `jupyter nbconvert --execute` runs all 31 cells with no errors or warnings; 5 figures written to `reports/figures/01_*.png`. All 21 checks pass after the warehouse fix. `pytest`: 43 passed (8 new in `tests/test_data.py`).
> Headline base rates: opened above issue 71% mainboard / 78% SME; median listing gain +9.6% / +10.2%; median 1-year return over NIFTY +17.0 pp / +2.5 pp (60% / 52% beat the index).
> Carried into Phase 4: treat listing gains at a ceiling as censored (use medians, ranks and threshold shares, not means or OLS); SME subscription analysis effectively starts in 2018; SME QIB analysis only from 2023; never use Tickertape valuation fields as explanations (measured at the snapshot).
> Carried into Phase 7: `ruff` flags import order in `tests/test_data.py`, because it treats `duckdb` as a local package (there is a `duckdb/` folder at the repo root).

---

## Phase 4 — Core analysis (`notebooks/02_subscription_vs_returns.ipynb`)

### Research questions — answer each separately for SME and mainboard
1. Does subscription demand predict listing-day gain?
2. Is QIB demand a stronger signal than retail or NII? (Mainboard, and SME issues that have a QIB category.)
3. Do listing pops persist or fade? 1-year excess return vs NIFTY by listing-gain quartile and by subscription quartile.
4. Does the relationship hold in hot vs cold IPO years?

### Methods — in `src/stats.py`, unit-tested in `tests/test_stats.py`
- [ ] `decile_table()` — median and IQR by subscription decile/quartile. Medians, not means: the data is heavily right-skewed.
- [ ] `bootstrap_spearman()` — rank correlation with 95% bootstrap CI.
- [ ] `bootstrap_median_diff()` — top vs bottom subscription quartile, 10,000 resamples, fixed seed.
- [ ] Confound check: stratify within year × segment (and sector for mainboard). Optional: `statsmodels` quantile regression of listing gain on log subscription with year, sector and log issue size controls.
- [ ] Report effect sizes in percentage points; note multiple comparisons.

### Charts — 5–6 maximum, saved to `reports/figures/`
- [ ] Median listing gain by subscription decile with IQR band, one panel per segment.
- [ ] QIB vs retail subscription against listing gain.
- [ ] Listing gain vs 1-year excess return (the fade).
- [ ] Base rate by listing year with counts annotated.
- [ ] Cohort excess return vs NIFTY.

### Verdict
- [ ] A plain-language answer to each question, with numbers and CIs. **Write it even if the answer is "no".**

**Done when:** every question has a numeric answer with a CI; notebook executed with outputs saved.

---

## Phase 5 — One small model (`notebooks/03_listing_gain_model.ipynb`)

- [ ] Target: `listing_gain_pct > 0`; sensitivity version at `> 10`.
- [ ] Features — pre-listing only: log subscription (total, QIB, NII, retail), `has_qib_category`, `segment`, log issue size, issue price, listing month.
- [ ] **Time split before any fitting:** train = listed ≤ 2023, test = 2024–2025.
- [ ] `sklearn` `Pipeline(ColumnTransformer(impute → scale / one-hot), LogisticRegression)` — every transform fit on train only.
- [ ] Baselines to beat: majority class; rule "total subscription > 10×".
- [ ] Metrics: ROC-AUC, Brier score, calibration plot; coefficients interpreted in plain language.
- [ ] Simple backtest: median and mean listing return when applying where `p > threshold` vs applying to every IPO.
- [ ] Caveats written out:
  - allotment probability collapses in heavily oversubscribed issues, so backtest returns aren't achievable as stated;
  - final-day subscription is only partly known when the apply decision is made.

**Done when:** results table vs baselines is reported honestly (including if the model doesn't beat them). No further models.

---

## Phase 6 — Streamlit dashboard (`app/streamlit_app.py`)

- [ ] Reads `data/mart/mart_ipo_analytics_<snapshot>.parquet` only — no scraping or DuckDB at runtime. Plotly charts, `@st.cache_data`.
- [ ] Pages:
  - **Overview** — KPI tiles (IPO count, median listing gain, % positive, by segment) and join funnel.
  - **Explore** — filters (segment, year, sector, issue size); subscription vs listing gain scatter (log x); decile chart.
  - **IPO lookup** — search a company: subscription breakdown, listing gain, returns vs NIFTY.
  - **Findings** — Phase 4 verdicts with their charts.
- [ ] Deploy to Streamlit Community Cloud (`app/requirements.txt`); put the public link at the top of the README.

**Done when:** `streamlit run app/streamlit_app.py` works locally and the deployed link loads.

---

## Phase 7 — Packaging and polish

- [ ] **README**, in this order:
  1. One-line summary.
  2. Headline findings in the first three lines.
  3. Dashboard link.
  4. Pipeline diagram: scrapers → raw snapshots → `stg_` → `int_` → `mart_` → checks → notebooks / app.
  5. Coverage and join funnel.
  6. Data dictionary for every mart column: type, source, definition, and the date each return is measured to.
  7. Findings with 4–5 charts.
  8. Model — one paragraph.
  9. Limitations.
  10. Reproduce: Python version, `--skip-scrape` rebuild, which sources are still live.
  11. Next steps.
- [ ] **Limitations** to state explicitly: survivorship (Tickertape lists survivors; NSE prices stop at delisting), single snapshot date, NSE feed starts 2016, structural QIB missingness for SME, allotment caveat, share of fuzzy/manual matches, returns exclude dividends, demergers, the SME listing cap from 2024-07-04.
- [ ] `reports/findings.md` — one page for a non-technical reader.
- [ ] CI: GitHub Actions workflow running `ruff` and `pytest` on fixture data (no scraping).
- [ ] Git: land work as logical per-phase commits. Set repo description and topics: `sql`, `duckdb`, `etl`, `analytics-engineering`, `web-scraping`, `streamlit`, `ipo`, `indian-stock-market`. Pin the repo.
- [ ] Clean up: delete or move this roadmap to `docs/`; remove stale logs from the tree.

**Done when:** someone can clone the repo, rebuild the mart from the saved snapshot, run the notebooks and the app, and see the headline finding within 30 seconds of opening the README.

---

## Stretch (after v1)

- [ ] Grey market premium (GMP) as a feature, with a note on source reliability.
- [ ] Anchor investor and promoter holding data from RHP filings.
- [ ] Port `stg_` / `int_` / `mart_` to dbt-duckdb with schema tests; scheduled monthly re-scrape.
