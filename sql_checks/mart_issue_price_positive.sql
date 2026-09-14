-- Every IPO has a positive issue price.
SELECT ticker, ipo_end_dt, issue_price
FROM mart_ipo_analytics
WHERE issue_price IS NULL OR issue_price <= 0;
