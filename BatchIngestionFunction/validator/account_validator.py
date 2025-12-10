# validator/account_validator.py
from ..utils.sanitizer import strip, to_float
from ..utils.date_utils import parse_ts


def validate_account_row(row: dict):
    """
    Validate account_master row.
    Returns list of errors.
    """

    errors = []
    acc = strip(row.get("AccountNumber"))
    cust = strip(row.get("CustomerID"))

    if not acc:
        errors.append("Missing AccountNumber")
    if not cust:
        errors.append("Missing CustomerID")

    # Balance
    bal = to_float(row.get("Balance"))
    if bal is None:
        errors.append("Invalid Balance")

    # AccountOpenDate
    dt = parse_ts(row.get("AccountOpenDate"))
    if dt is None:
        errors.append("Invalid AccountOpenDate")

    return errors
