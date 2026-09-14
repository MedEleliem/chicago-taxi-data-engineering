"""Date range validation shared by Airflow and the standalone runner."""
from datetime import date, timedelta


def processing_dates(start_date, end_date, max_days=92):
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    days = (end - start).days + 1
    if days > max_days:
        raise ValueError(f"date range exceeds the {max_days}-day limit")
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days)]
