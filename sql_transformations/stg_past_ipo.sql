-- One row per equity issue in the NSE past-issues feed.
-- Which security types are equity, and which segment each belongs to, is defined in
-- data/seeds/security_types.csv: a type with no segment is dropped here (DEBT, NCDs, REITs, InvITs).
-- The seed is the contract with the feed's vocabulary. NSE collapsed EQ and BE into EQUITY during 2026,
-- so both spellings are listed; sql_checks/past_ipo_known_security_types.sql fails on any code not in it.
-- Issues that are not IPOs (FPOs, withdrawn, partly-paid, follow-ons) are kept but carry an
-- exclusion_reason, so the join funnel can count them. Whether an IPO listed is decided in int_ipo_listing.

CREATE OR REPLACE TABLE stg_past_ipo AS
WITH parsed AS (
    SELECT
        TRIM(symbol)                                                  AS ticker,
        TRIM(company)                                                 AS company_name,
        norm_company_name(company)                                    AS company_name_norm,
        UPPER(TRIM(securitytype))                                     AS security_type,
        t.segment                                                     AS segment,
        TRY_STRPTIME(TRIM(ipostartdate), '%d-%b-%Y')::DATE            AS ipo_start_dt,
        TRY_STRPTIME(TRIM(ipoenddate), '%d-%b-%Y')::DATE              AS ipo_end_dt,
        TRY_STRPTIME(NULLIF(TRIM(listingdate), '-'), '%d-%b-%Y')::DATE AS listing_dt_reported,
        TRIM(pricerange)                                              AS price_range_raw,
        list_transform(
            regexp_extract_all(pricerange, '\d+(?:\.\d+)?'),
            x -> TRY_CAST(x AS DECIMAL(12, 2))
        )                                                             AS band,
        TRY_CAST(TRIM(issueprice) AS DECIMAL(12, 2))                  AS issueprice_reported
    FROM raw_past_ipo
    JOIN seed_security_types t ON t.security_type = UPPER(TRIM(securitytype))
    WHERE t.segment IS NOT NULL
),

priced AS (
    SELECT
        * EXCLUDE (band),
        list_min(band) AS issue_price_band_low,
        list_max(band) AS issue_price_band_high
    FROM parsed
),

-- The feed's issueprice is only trustworthy inside the price band. Outside it, it is usually a
-- post-migration or split-adjusted price (e.g. NPST: 2,177 against a Rs 76-80 band).
issue_price AS (
    SELECT
        *,
        CASE
            WHEN issueprice_reported BETWEEN issue_price_band_low AND issue_price_band_high
                THEN issueprice_reported
            WHEN issue_price_band_high IS NOT NULL THEN issue_price_band_high
            ELSE issueprice_reported
        END AS issue_price,
        CASE
            WHEN issueprice_reported BETWEEN issue_price_band_low AND issue_price_band_high
                THEN 'feed_issue_price'
            WHEN issue_price_band_high IS NOT NULL THEN 'band_upper'
            WHEN issueprice_reported IS NOT NULL THEN 'feed_issue_price_no_band'
        END AS issue_price_source
    FROM priced
),

excluded AS (
    SELECT
        i.*,
        CASE
            WHEN regexp_matches(i.company_name, '(?i)withdraw') THEN 'withdrawn'
            WHEN regexp_matches(i.company_name, '(?i)\bFPO\b')  THEN 'fpo'
            WHEN s.reason IS NOT NULL                          THEN s.reason
        END AS name_or_seed_exclusion
    FROM issue_price i
    LEFT JOIN seed_excluded_issues s
        ON s.ticker = i.ticker
       AND (s.ipo_end_dt IS NULL OR s.ipo_end_dt = i.ipo_end_dt)
),

-- The first remaining issue per ticker is the IPO; anything later is a follow-on.
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, name_or_seed_exclusion IS NULL
            ORDER BY ipo_end_dt, ipo_start_dt, listing_dt_reported
        ) AS issue_seq
    FROM excluded
)

SELECT
    ticker,
    company_name,
    company_name_norm,
    security_type,
    segment,
    ipo_start_dt,
    ipo_end_dt,
    listing_dt_reported,
    -- NSE reports the SME -> mainboard migration date as the listing date for migrated companies,
    -- years after the issue. Only a listing within 30 days of issue close is treated as the listing.
    CASE WHEN listing_dt_reported - ipo_end_dt BETWEEN 0 AND 30 THEN listing_dt_reported END AS listing_dt,
    CASE
        WHEN listing_dt_reported IS NULL                   THEN 'not_listed'
        WHEN ipo_end_dt IS NULL                            THEN 'no_issue_close_date'
        WHEN listing_dt_reported < ipo_end_dt              THEN 'listing_before_close'
        WHEN listing_dt_reported - ipo_end_dt > 30         THEN 'listing_date_is_later_event'
        ELSE 'ok'
    END AS listing_dt_flag,
    price_range_raw,
    issue_price_band_low,
    issue_price_band_high,
    issueprice_reported,
    issue_price,
    issue_price_source,
    COALESCE(
        name_or_seed_exclusion,
        CASE WHEN issue_seq > 1 THEN 'follow_on' END
    ) AS exclusion_reason
FROM ranked;
