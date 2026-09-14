# Data dictionary — `mart_ipo_analytics`

One row per NSE-listed equity IPO, mainboard and SME: 1,143 rows in snapshot 2026-02-15. Exported to `data/mart/mart_ipo_analytics_<snapshot>.parquet` and `.csv`; built by [`sql_transformations/mart_ipo_analytics.sql`](../sql_transformations/mart_ipo_analytics.sql).

**Conventions**
- **Missing data is never dropped.** An IPO stays in the mart with NULLs, and the `has_*` flags say what is available.
- **Returns** are price returns in percent, adjusted for bonuses, splits and consolidations, not for dividends.
- **`excess_*` columns are in percentage points** even though their names end in `_pct`.
- **Snapshot-date columns** describe the company when the data was scraped, not at listing.
- **Types** are the DuckDB types; read with `pd.read_parquet`, dates arrive as `datetime.date`, decimals as `Decimal` and nullable booleans as objects. `src.data.load_mart()` converts them to datetime64, float and nullable boolean, and adds `listing_year`.

**Sources**
- NSE = NSE past-issues feed.
- Bhavcopy = NSE daily CM bhavcopy.
- Chittorgarh = Chittorgarh subscription reports.
- Tickertape = Tickertape screener.
- NIFTY = NSE index closing file.

## Issue

| Column | Type | Source | Definition |
|---|---|---|---|
| `ticker` | text | NSE | Symbol in the past-issues feed, at the time of the IPO. With `ipo_end_dt` it identifies an IPO. |
| `company_name` | text | NSE | Company name as in the feed. |
| `segment` | text | NSE | `MAINBOARD` or `SME`, from the security type. Analyse the two separately. |
| `security_type` | text | NSE | `EQ`, `BE` (mainboard, trade-for-trade) or `SME`. Debt, REITs, InvITs and warrants are excluded upstream. |
| `ipo_start_dt` | date | NSE | Issue open date. |
| `ipo_end_dt` | date | NSE | Issue close date, the day the issue price is paid. |
| `issue_year` | integer | derived | Year of `ipo_end_dt`. |
| `issue_price` | decimal(12,2) | NSE, Bhavcopy | Price per share paid by allottees (₹). The feed's issue price when it lies inside the price band, otherwise the band's upper end. If the listing-day base price differs but is one of the feed's prices for the issue, the base price is used (4 issues). |
| `issue_price_band_low`, `issue_price_band_high` | decimal(12,2) | NSE | Price band (₹); equal for fixed-price issues. |
| `issue_price_source` | text | derived | `feed_issue_price`, `band_upper`, `listing_base_price` or `feed_issue_price_no_band`. |
| `snapshot_date` | date | pipeline | Date of the raw snapshot the row was built from. |

## Listing day

| Column | Type | Source | Definition |
|---|---|---|---|
| `listing_dt` | date | Bhavcopy | First NSE trading day of the IPO's security within 30 days of issue close. NULL for the two BSE-only listings (CAMS, PROTEAN). |
| `listing_dt_source` | text | derived | `nse_bhavcopy` when found in the bhavcopy. |
| `listing_dt_reported` | date | NSE | Listing date as the feed reports it. For migrated SME companies this is often the later mainboard migration date. |
| `listing_dt_flag` | text | derived | Check of the reported date: `ok`, `listing_date_is_later_event` (more than 30 days after close), `not_listed` or `listing_before_close`. |
| `has_listing_price` | boolean | Bhavcopy | The IPO has a listing-day row in the NSE bhavcopy. |
| `listing_method` | text | derived | How the listing day was found. `feed_symbol`: the feed's symbol starts trading. `new_listing`: another symbol's first trade at a base price equal to the issue price. `manual`: reviewed in `data/seeds/manual_listings.csv`. |
| `listing_symbol` | text | Bhavcopy | Symbol the IPO traded under on its listing day. |
| `listing_base_price` | double | Bhavcopy | Previous close in the bhavcopy on the listing day, i.e. the exchange's base price (₹). |
| `listing_base_matches_issue_price` | boolean | derived | `listing_base_price` equals `issue_price`. |
| `listing_open` | double | Bhavcopy | Listing-day open (₹). |
| `listing_close` | double | Bhavcopy | Listing-day close (₹). |
| `listing_volume` | double | Bhavcopy | Shares traded on the listing day. |
| `listing_gain_pct` | double | derived | **The listing gain:** `listing_open / issue_price − 1`, in %. |
| `listing_day_close_gain_pct` | double | derived | `listing_close / issue_price − 1`, in %. |
| `listing_cap_applies` | boolean | derived | SME IPO listed on or after 2024-07-04, when NSE capped the SME opening price at +90% over issue price. NULL for mainboard or no listing price. |
| `listing_capped` | boolean | derived | Opened at that cap (`listing_open ≥ 1.9 × issue_price − ₹0.05`). NULL where the cap doesn't apply. The observed +20% ceiling for SME listings before 2018-04-26 is not a column; see `src.data.at_listing_ceiling()`. |

## Subscription

| Column | Type | Source | Definition |
|---|---|---|---|
| `has_subscription_data` | boolean | derived | Matched to a Chittorgarh subscription record. |
| `has_subscription_figures` | boolean | Chittorgarh | The matched record has a total subscription multiple. Most 2016–17 SME records don't. |
| `subscription_match_method` | text | derived | `exact` (same normalised name and close date), `fuzzy` (close date within ±3 days, Jaro-Winkler similarity ≥ 0.90) or `manual` (`data/seeds/manual_subscription_matches.csv`). |
| `subscription_match_score` | double | derived | Jaro-Winkler similarity of the normalised names (1.0 for exact). |
| `issue_size_cr` | double | Chittorgarh | Total issue amount including firm reservations, ₹ crore. |
| `subscription_total_x` | double | Chittorgarh | Times the whole issue was subscribed, on the final day. |
| `subscription_qib_x` | double | Chittorgarh | Qualified institutional buyers, times subscribed. NULL when the issue has no QIB portion (see `has_qib_category`) or the figure is unknown. |
| `subscription_nii_x` | double | Chittorgarh | Non-institutional investors (HNIs), times subscribed. |
| `subscription_snii_x`, `subscription_bnii_x` | double | Chittorgarh | Small (₹2–10 lakh) and big (above ₹10 lakh) NII, times subscribed; reported for recent issues only. |
| `subscription_retail_x` | double | Chittorgarh | Retail individual investors, times subscribed. |
| `subscription_employee_x` | double | Chittorgarh | Employee reservation, times subscribed, where there was one. |
| `applications` | double | Chittorgarh | Number of applications received, where reported. |
| `has_qib_category` | boolean | Chittorgarh | The issue had a QIB portion. Many SME issues don't, which is a category, not a zero. NULL when there are no subscription figures. |

Chittorgarh's placeholder `9999` for an unknown multiple is stored as NULL.

## Company at the snapshot date (Tickertape)

| Column | Type | Source | Definition |
|---|---|---|---|
| `has_market_data` | boolean | derived | Matched to a Tickertape stock. |
| `market_match_method` | text | derived | `nse_symbol` (the Tickertape ticker is the IPO's current NSE symbol, found by following renames in the bhavcopy), `ticker` (same ticker, best name similarity), `name` (identical normalised name) or `manual` (`data/seeds/manual_ticker_matches.csv`). |
| `market_match_score` | double | derived | Jaro-Winkler similarity of the normalised names. |
| `tickertape_sid` | text | Tickertape | Tickertape stock id. |
| `current_ticker` | text | Tickertape | Ticker at the snapshot date; differs from `ticker` after a rename. |
| `current_company_name` | text | Tickertape | Company name at the snapshot date. |
| `sector`, `subindustry` | text | Tickertape | Classification at the snapshot date. |
| `market_cap_cr` | double | Tickertape | Market capitalisation, ₹ crore. |
| `current_price` | double | Tickertape | Last price (₹). Not adjusted to the IPO share; use the return columns instead. |
| `pe_ratio`, `pb_ratio` | double | Tickertape | Price-to-earnings and price-to-book. NULL when unknown, never 0. |
| `roe_pct` | double | Tickertape | Return on equity, %. |
| `is_lossmaking` | boolean | derived | `pe_ratio < 0` or `roe_pct < 0`; NULL if both are unknown. |
| `return_4w_pct`, `return_1d_pct` | double | Tickertape | Four-week and one-day price return to the scrape date, %. |

## Price history

| Column | Type | Source | Definition |
|---|---|---|---|
| `last_trade_dt` | date | Bhavcopy | Last NSE trade of the IPO's security on or before the snapshot date, following renames, ISIN changes and SME → mainboard migration. |
| `is_trading_at_snapshot` | boolean | derived | `last_trade_dt` is within 30 days of the snapshot date. |
| `share_multiplier` | double | Bhavcopy, NSE corporate actions | Shares held at `last_trade_dt` per share allotted at the IPO (bonuses and splits multiply it, consolidations divide it). |
| `corporate_action_count` | integer | NSE corporate actions | Bonus, split and consolidation actions with an ex-date up to the snapshot. |
| `price_gap_dt` | date | derived | First overnight move that corporate actions don't explain (adjusted open below 60% or above 167% of the previous close): usually a demerger. Returns measured across it are NULL. |
| `holding_days` | integer | derived | `last_trade_dt − ipo_end_dt`. |
| `holding_years` | double | derived | `holding_days / 365.25`. |

## Returns

Every fixed-horizon return uses the **horizon date** `listing_dt + 1, 6, 12 or 36 months`, and the **horizon price** is the last adjusted close on or before that date. A horizon return is NULL when:
- the horizon date is after the snapshot date;
- the stock had no NSE trade in the 30 days before it;
- an unexplained price gap lies between listing and the horizon price;
- or the IPO has no NSE listing.

For each `{h}` in `1m`, `6m`, `1y` and `3y`:

| Column | Type | From → to | Definition |
|---|---|---|---|
| `return_{h}_pct` | double | issue price → horizon price | What an allottee made, %. |
| `nifty_return_{h}_pct` | double | NIFTY close on or before `ipo_end_dt` → NIFTY close on or before the horizon price date | NIFTY 50 over the same money-at-risk period, %. |
| `excess_return_{h}_pct` | double | as above | `return_{h}_pct − nifty_return_{h}_pct`, **percentage points**. |
| `aftermarket_excess_return_{h}_pct` | double | listing-day close → horizon price, against NIFTY from `listing_dt` | Buying at the listing-day close instead of being allotted, minus NIFTY 50 over the same period, **percentage points**. |

| Column | Type | From → to | Definition |
|---|---|---|---|
| `return_since_issue_pct` | double | issue price → adjusted close at `last_trade_dt` | Return to the snapshot, %. **Not comparable across listing years**, because the holding period differs (see notebook 01). NULL after an unexplained price gap. |
| `annualized_return_pct` | double | as above | `return_since_issue_pct` annualised over `holding_days`; NULL when held under 182 days or after a price gap. |

## Related tables

- **`audit_join_funnel`** is exported to `reports/join_funnel_<snapshot>.csv`. It gives row counts by segment at each stage, from the raw NSE feed to the mart.
- **Intermediate tables** (`int_ipo_listing`, `int_ipo_price_history`, `int_ipo_horizon_returns`, and the two match tables) live in `data/duckdb/ipo.duckdb` after a pipeline run. They aren't exported.
