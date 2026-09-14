-- Subscription data is found for enough IPOs in each segment.
SELECT segment, AVG(has_subscription_data::INT) AS coverage, getvariable('min_subscription_coverage') AS required
FROM mart_ipo_analytics
GROUP BY segment
HAVING AVG(has_subscription_data::INT) < getvariable('min_subscription_coverage');
