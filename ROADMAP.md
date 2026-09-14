# IPO Analysis — Roadmap

The single source of truth for finishing this project. Work top to bottom, tick boxes as you go, and commit at the end of each phase. When Phase 7 is done, delete this file or move it to `docs/`.

**End state:** a trustworthy warehouse → a clearly stated question answered with the right statistics → one small, honest model → a live Streamlit dashboard → a README that leads with the finding.

**Scope decisions (fixed):**
- SME and mainboard IPOs are both analysed, **segmented in every cut, never pooled**.
- The ML scaffolding is removed. The only model is one leakage-free logistic regression (Phase 5).
- Dashboard: Streamlit.

---

## Why this roadmap exists — verified problems

Checked against `data/staged/mart_ipo_analytics_20260215.csv` (snapshot 2026-02-15) and `data/duckdb/ipo.duckdb`.

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

Files: `sql_transformations/stg_*.sql`, new `sql_transformations/int_*.sql`, `sql_transformations/mart_ipo_analytics.sql`, `pipeline.py`, new `sql_checks/`, new `data/seeds/`, new `tests/`.

### 1.1 `stg_past_ipo`
- [ ] Filter `securitytype IN ('EQ', 'BE', 'SME')`.
- [ ] `issue_price` = `TRY_CAST(TRIM(issueprice) AS DECIMAL(10,2))`; fall back to the price-band regex only when null.
- [ ] Add `segment` (`SME` if security type is SME, else `MAINBOARD`).
- [ ] Add `company_name_norm`: upper-case, strip `LIMITED|LTD|PVT|PRIVATE|THE|INDIA|CO|COMPANY` and all non-alphanumerics.
- [ ] Add `date_quality_flag` for impossible dates (listing before issue close — e.g. Ruchi Soya: 2022 issue, 2003 listing).
- [ ] Drop the `_1` suffix from all table names (and remove the stale unsuffixed tables).

### 1.2 `stg_ipo_subscription`
- [ ] `company_name = COALESCE(Company, "Company Name")`.
- [ ] `ipo_end_dt = COALESCE("~Issue_Close_Date", TRY_STRPTIME(COALESCE("Closing Date", "Close Date"), '%b %d, %Y'))`; `ipo_start_dt` may be null.
- [ ] Keep `sNII (x)`, `bNII (x)`, `Applications`.
- [ ] Add `company_name_norm`.
- [ ] Dedupe on `(company_name_norm, ipo_end_dt)` with a deterministic `ORDER BY`.

### 1.3 `stg_tickertape`
- [ ] Keep `sid` and `name`.
- [ ] Remove `COALESCE(..., 0)` on `pe`, `roe`, `pb` — leave NULL.
- [ ] Add `is_lossmaking = (pe < 0 OR roe < 0)`; NULL when both unknown.
- [ ] Add `company_name_norm`.

### 1.4 `int_ipo_subscription_match` (new)
- [ ] Candidate pairs: `ABS(ipo.ipo_end_dt − sub.ipo_end_dt) ≤ 3` and `jaro_winkler_similarity(norm, norm) ≥ 0.90`.
- [ ] Keep the best candidate per IPO **and** per subscription row (one-to-one via `ROW_NUMBER`).
- [ ] Store `match_score`, `match_method` (`exact` / `fuzzy` / `manual`).
- [ ] Apply overrides from `data/seeds/manual_subscription_matches.csv`.

### 1.5 `int_ipo_market_match` (new)
- [ ] Join on ticker, then rank candidates by name similarity; accept only the best, and only above threshold.
- [ ] Seed `data/seeds/manual_ticker_matches.csv` for legitimate renames: Adani Wilmar → AWL Agri Business, Schloss Bangalore → Leela Palaces Hotels & Resorts, Empyrean Cashews → Krishival Foods, IKIO Lighting → IKIO Technologies (review the rest of the low-similarity list by hand).
- [ ] Unmatched IPOs are **kept** with `has_market_data = false`.

### 1.6 Rebuild `mart_ipo_analytics`
- [ ] Base = all listed equity IPOs; `LEFT JOIN` both match tables. Delete the `WHERE` that silently drops rows.
- [ ] Flags: `has_subscription_data`, `has_market_data`, `has_qib_category` ("no QIB category" is a level, never zero).
- [ ] Rename `listing_gains_pct` → `return_since_issue_pct`; add `snapshot_date` (scrape date, not `CURRENT_DATE`).
- [ ] Add `holding_days`, `holding_years`, `annualized_return_pct` (NULL when `holding_days < 182`).
- [ ] Export `data/mart/mart_ipo_analytics.parquet` (committed) plus the dated CSV.

### 1.7 Join funnel
- [ ] `audit_join_funnel` table: row counts at each stage (raw → equity filter → listed → subscription match → market match → mart). Log it; export `reports/join_funnel.csv`.

### 1.8 Data-quality checks (pipeline stage 5 — fail loudly)
Each `sql_checks/*.sql` returns violating rows; any row fails the run.
- [ ] `(ticker, listing_dt)` unique.
- [ ] `issue_price > 0`.
- [ ] `listing_dt >= ipo_end_dt` (excluding flagged rows).
- [ ] All accepted matches above threshold.
- [ ] Mart row count ≥ 1,000.
- [ ] Subscription coverage ≥ 90%.
- [ ] Mainboard QIB null rate = 0%.

### 1.9 Tests
- [ ] `tests/test_warehouse.py` runs stages 2–5 against small CSVs in `tests/fixtures/`, covering:
  - GSTL-style ticker collision resolves to the right company;
  - second subscription layout's dates parse and join;
  - debt / REIT / InvIT rows excluded;
  - no duplicate `(ticker, listing_dt)`.

**Done when:** pipeline passes all checks, mart has ~1,050–1,100 rows, funnel is published, `pytest` is green.

---

## Phase 2 — Listing-day prices and NIFTY benchmark

- [ ] **Coverage spike first:** fetch 50 random tickers (25 SME, 25 mainboard) via `yfinance` as `SYMBOL.NS`; record hit rate. Use the current Tickertape ticker where the company was renamed.
- [ ] New `scrapers/price_history_scraper.py`: daily OHLC from `listing_dt` to snapshot date per IPO, plus `^NSEI` (NIFTY 50). Per-ticker cache, retry with backoff, output `data/raw/<snapshot>/price_history.parquet`.
- [ ] **Fallback** if SME coverage < ~85%: listing-day open/close from NSE daily bhavcopy archives (CM + SME files) for each distinct listing date (~1,100 dates).
- [ ] Verify the NSE SME pre-open listing-price cap (90% over issue price, introduced ~2025 — confirm the circular and effective date). Add `listing_capped` flag.
- [ ] New `stg_price_history`.
- [ ] New mart columns:
  - `listing_open`, `listing_close`
  - `listing_gain_pct` — open vs issue price (the real listing gain)
  - `listing_day_close_gain_pct`
  - `return_1m_pct`, `return_6m_pct`, `return_1y_pct`, `return_3y_pct` from issue price (NULL if the horizon hasn't elapsed)
  - matching NIFTY returns and `excess_return_*`
- [ ] Checks: listing price present for ≥ 85% of mainboard and ≥ 75% of SME; listing open within sanity bounds.
- [ ] Add price coverage to the join funnel.

**Done when:** true listing gain and fixed-horizon excess returns are in the mart with coverage reported.

---

## Phase 3 — Data overview notebook (`notebooks/01_data_overview.ipynb`)

- [ ] Shared helpers:
  - `src/config.py` — paths, snapshot date
  - `src/data.py` — `load_mart()` from Parquet
  - `src/plotting.py` — one chart style, `save_fig()` → `reports/figures/`
- [ ] Coverage: join funnel, IPO count by year × segment, missingness by segment (QIB is structural for SME).
- [ ] Distributions of `listing_gain_pct` and `excess_return_1y_pct` (log/symlog axis, median marked).
- [ ] Base rates by year and segment: share positive, median, IQR.
- [ ] One chart showing why `return_since_issue_pct` can't be compared across years (median return by listing year next to holding period).
- [ ] Closing markdown: what this data can and can't support.

**Done when:** notebook runs top to bottom, outputs saved in the file.

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

- [ ] Reads `data/mart/mart_ipo_analytics.parquet` only — no scraping or DuckDB at runtime. Plotly charts, `@st.cache_data`.
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
- [ ] **Limitations** to state explicitly: survivorship (Tickertape lists survivors), single snapshot date, NSE feed starts 2016, structural QIB missingness for SME, allotment caveat, share of fuzzy/manual matches.
- [ ] `reports/findings.md` — one page for a non-technical reader.
- [ ] CI: GitHub Actions workflow running `ruff` and `pytest` on fixture data (no scraping).
- [ ] Git: land work as logical per-phase commits. Set repo description and topics: `sql`, `duckdb`, `etl`, `analytics-engineering`, `web-scraping`, `streamlit`, `ipo`, `indian-stock-market`. Pin the repo.
- [ ] Clean up: delete or move this roadmap to `docs/`; remove stale staged CSVs and logs from the tree.

**Done when:** someone can clone the repo, rebuild the mart from the saved snapshot, run the notebooks and the app, and see the headline finding within 30 seconds of opening the README.

---

## Stretch (after v1)

- [ ] Grey market premium (GMP) as a feature, with a note on source reliability.
- [ ] Anchor investor and promoter holding data from RHP filings.
- [ ] Port `stg_` / `int_` / `mart_` to dbt-duckdb with schema tests; scheduled monthly re-scrape.
