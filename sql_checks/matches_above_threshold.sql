-- Name-based matches meet their similarity thresholds (manual seed matches and matches on the current
-- NSE symbol, which rest on the ISIN chain rather than the name, are exempt).
SELECT 'subscription' AS match_table, ticker, ipo_end_dt, match_method, match_score
FROM int_ipo_subscription_match
WHERE match_method <> 'manual' AND match_score < getvariable('subscription_match_min_score')
UNION ALL
SELECT 'market', ticker, ipo_end_dt, match_method, match_score
FROM int_ipo_market_match
WHERE match_method NOT IN ('manual', 'nse_symbol') AND match_score < getvariable('market_match_min_score');
