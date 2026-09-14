-- From 2024-07-04 an SME IPO cannot open more than 90% above its issue price (NSE special pre-open cap).
SELECT ticker, ipo_end_dt, listing_dt, issue_price, listing_open
FROM mart_ipo_analytics
WHERE listing_cap_applies
  AND listing_open > ROUND(issue_price * 1.9, 2) + 0.05;
