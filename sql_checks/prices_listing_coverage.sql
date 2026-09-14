-- Enough IPOs in each segment have listing-day prices from the NSE bhavcopy.
SELECT
    segment,
    AVG(has_listing_price::INT) AS coverage,
    CASE segment
        WHEN 'SME' THEN getvariable('min_listing_price_coverage_sme')
        ELSE getvariable('min_listing_price_coverage_mainboard')
    END AS required
FROM mart_ipo_analytics
GROUP BY segment
HAVING AVG(has_listing_price::INT) < required;
