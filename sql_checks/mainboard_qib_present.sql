-- Every mainboard issue has a QIB category, so a missing one means a parsing problem.
SELECT ticker, ipo_end_dt, company_name
FROM mart_ipo_analytics
WHERE segment = 'MAINBOARD' AND has_subscription_figures AND NOT has_qib_category;
