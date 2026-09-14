-- Every mainboard issue has a QIB category, so a missing QIB figure means a parsing problem.
SELECT ticker, ipo_end_dt, company_name
FROM mart_ipo_analytics
WHERE segment = 'MAINBOARD' AND has_subscription_data AND subscription_qib_x IS NULL;
