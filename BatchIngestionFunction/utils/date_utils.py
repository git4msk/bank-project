# utils/date_utils.py
from datetime import datetime, timezone
import logging
from dateutil import parser as date_parser


def parse_ts(value):
    """
    Parse timestamp or date string into an aware datetime (UTC).
    Returns None if parse fails.
    """
    if not value:
        return None

    try:
        dt = date_parser.parse(value)
    except Exception:
        logging.warning(f"Failed to parse datetime: {value}")
        return None

    # make timezone aware (UTC) if naive
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def utcnow():
    """Return aware UTC datetime."""
    return datetime.now(timezone.utc)
