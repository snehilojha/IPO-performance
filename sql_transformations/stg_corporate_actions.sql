-- Bonus issues, face-value splits and consolidations from NSE corporate actions, one row per action.
-- share_factor = shares held after the action per share held before it; prices divide by it on the ex-date.
--   Bonus a:b              -> (a + b) / b      (a new shares for every b held)
--   Split Rs X to Rs Y     -> X / Y
--   Consolidation X to Y   -> X / Y            (below 1)
-- A subject can announce several actions at once ("Bonus 1:1/Face Value Split ..."), giving several rows.
-- Bonus preference shares (NCRPS) and bonus debentures are not equity and are ignored.
-- Reviewed actions missing from the API are added from data/seeds/manual_corporate_actions.csv.

CREATE OR REPLACE TABLE stg_corporate_actions AS
WITH parsed AS (
    SELECT
        UPPER(TRIM(symbol))                                              AS symbol,
        TRIM(series)                                                     AS series,
        TRIM(company)                                                    AS company_name,
        TRY_STRPTIME(TRIM(ex_date), '%d-%b-%Y')::DATE                    AS ex_dt,
        TRIM(subject)                                                    AS subject,
        source_index,
        regexp_matches(UPPER(subject), 'NCRPS|DEBENTURE|PREFERENCE')     AS is_non_equity,
        regexp_extract(UPPER(subject), 'BONUS\s*-?\s*([\d.]+)\s*:\s*([\d.]+)', ['new', 'held']) AS bonus,
        regexp_extract(UPPER(subject), 'SPLIT.*?FROM\s*R[SE]\.?\s*([\d.]+).*?TO\s*R[SE]\.?\s*([\d.]+)',
                       ['from_fv', 'to_fv'])                             AS split,
        regexp_extract(UPPER(subject), 'CONSOLIDATION.*?R[SE]\.?\s*([\d.]+).*?TO\s*R[SE]\.?\s*([\d.]+)',
                       ['from_fv', 'to_fv'])                             AS consolidation
    FROM raw_corporate_actions
),

actions AS (
    SELECT symbol, series, company_name, ex_dt, subject, source_index, 'bonus' AS action_type,
           (TRY_CAST(bonus.new AS DOUBLE) + TRY_CAST(bonus.held AS DOUBLE)) / TRY_CAST(bonus.held AS DOUBLE) AS share_factor
    FROM parsed WHERE bonus.new <> '' AND NOT is_non_equity
    UNION ALL
    SELECT symbol, series, company_name, ex_dt, subject, source_index, 'split',
           TRY_CAST(split.from_fv AS DOUBLE) / TRY_CAST(split.to_fv AS DOUBLE)
    FROM parsed WHERE split.from_fv <> ''
    UNION ALL
    SELECT symbol, series, company_name, ex_dt, subject, source_index, 'consolidation',
           TRY_CAST(consolidation.from_fv AS DOUBLE) / TRY_CAST(consolidation.to_fv AS DOUBLE)
    FROM parsed WHERE consolidation.from_fv <> ''
    UNION ALL
    SELECT UPPER(TRIM(symbol)), NULL, NULL, ex_dt, note, 'manual', LOWER(TRIM(action_type)), share_factor
    FROM seed_manual_corporate_actions
)

-- The same action is sometimes announced twice (revised purpose, or listed under both indices)
SELECT symbol, series, company_name, ex_dt, action_type, ROUND(share_factor, 6) AS share_factor, subject, source_index
FROM actions
WHERE ex_dt IS NOT NULL AND share_factor > 0 AND share_factor <> 1
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol, ex_dt, action_type, ROUND(share_factor, 6)
    ORDER BY source_index, subject
) = 1;
