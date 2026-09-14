-- Shared macros used by the staging and matching models.

-- Company name reduced to a comparable key:
-- upper-case, '&' -> AND, punctuation removed, legal-form / filler words removed, spaces removed.
-- 'Globe Textiles (India) Limited' -> 'GLOBETEXTILES'; 'Kronox Lab SciencesLimited' -> 'KRONOXLABSCIENCES'
CREATE OR REPLACE MACRO norm_company_name(s) AS
    regexp_replace(
        replace(
            regexp_replace(
                regexp_replace(upper(replace(s, '&', ' AND ')), '[^A-Z0-9]+', ' ', 'g'),
                '\b(LIMITED|LTD|PVT|PRIVATE|THE|INDIA|CO|COMPANY)\b', ' ', 'g'
            ),
            ' ', ''
        ),
        '(LIMITED|LTD)$', ''
    );

-- Numbers scraped as text, e.g. '2,11,219' or '72\r'
CREATE OR REPLACE MACRO parse_number(s) AS
    TRY_CAST(NULLIF(regexp_replace(CAST(s AS VARCHAR), '[^0-9.]', '', 'g'), '') AS DOUBLE);
