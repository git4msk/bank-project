# utils/csv_utils.py
import csv
import io
import logging


def parse_csv(text: str):
    """
    Parse CSV text into a list of dicts using csv.Sniffer to auto-detect delimiter.
    Returns a list of row dictionaries.
    """
    if not text:
        return []

    try:
        first_line = text.splitlines()[0]
        dialect = csv.Sniffer().sniff(first_line)
    except Exception:
        logging.warning("Failed to detect dialect, falling back to default comma delimiter")
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    return [row for row in reader]


def detect_source_type(file_name: str) -> str:
    """
    Detects ingestion type based on filename keywords.
    Should match: ATM, UPI, ACCOUNT, CUSTOMER.
    """
    f = file_name.lower()
    if "atm" in f:
        return "ATM"
    if "upi" in f:
        return "UPI"
    if "account" in f:
        return "ACCOUNT"
    if "customer" in f:
        return "CUSTOMER"
    return "UNKNOWN"
