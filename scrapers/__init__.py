from datetime import timedelta, timezone
from functools import lru_cache
from pathlib import Path

import pandas as pd

# Indian Standard Time (no daylight saving). Snapshot and trading dates follow the NSE calendar.
IST = timezone(timedelta(hours=5, minutes=30), 'IST')

SECURITY_TYPE_SEED = Path(__file__).resolve().parent.parent / 'data' / 'seeds' / 'security_types.csv'


@lru_cache(maxsize=1)
def equity_security_types() -> tuple[str, ...]:
    """
    The feed's security-type codes that are equity IPOs, upper-cased, from data/seeds/security_types.csv.

    The same seed drives sql_transformations/stg_past_ipo.sql, so the scrapers and the warehouse cannot
    disagree about which issues are equity. NSE renames these codes (EQ and BE became EQUITY during 2026),
    which is why they are not hard-coded here.
    """
    seed = pd.read_csv(SECURITY_TYPE_SEED)
    equity = seed.loc[seed['segment'].notna(), 'security_type']
    return tuple(equity.str.strip().str.upper())
