-- Schema contract with the NSE past-issues feed.
-- Every security type the feed returns must be classified in data/seeds/security_types.csv, which decides
-- whether the issue is equity (and its segment) or is dropped. An unclassified code means NSE changed its
-- vocabulary, and the issues under it would be silently dropped from the mart.
-- This happened in 2026: EQ and BE were replaced by EQUITY, IV by INVIT and RR by REIT. Until the seed
-- listed them, a fresh scrape kept the SME rows and lost every mainboard IPO without any error.
-- To fix a failure: look at the code, decide whether it is an equity IPO, and add a row to the seed.
SELECT
    UPPER(TRIM(securitytype)) AS security_type,
    COUNT(*)                  AS issue_rows,
    MIN(TRIM(company))        AS example_company
FROM raw_past_ipo
WHERE UPPER(TRIM(securitytype)) NOT IN (SELECT security_type FROM seed_security_types)
GROUP BY 1
ORDER BY issue_rows DESC;
