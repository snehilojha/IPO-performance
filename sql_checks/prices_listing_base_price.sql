-- On the listing day the exchange's base price (the bhavcopy's previous close) equals the issue price for
-- nearly every IPO in each segment; a lower share means wrong listing days or wrong issue prices.
SELECT
    segment,
    AVG(listing_base_matches_issue_price::INT) AS share_matching,
    getvariable('min_listing_base_price_match') AS required
FROM mart_ipo_analytics
WHERE has_listing_price
GROUP BY segment
HAVING AVG(listing_base_matches_issue_price::INT) < getvariable('min_listing_base_price_match');
