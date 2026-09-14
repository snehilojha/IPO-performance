from datetime import timedelta, timezone

# Indian Standard Time (no daylight saving). Snapshot and trading dates follow the NSE calendar.
IST = timezone(timedelta(hours=5, minutes=30), 'IST')
