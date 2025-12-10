# validator/transaction_validator.py
from ..utils.date_utils import parse_ts
from ..utils.sanitizer import strip, to_float


def validate_transaction_row(row: dict, source_type: str):
    """
    Validate ATM/UPI transaction row.
    Returns (errors list, cleaned_row dict)
    """

    cleaned = {k.strip(): strip(v) for k, v in row.items()}
    errors = []

    # Mandatory: TransactionID
    txn_id = cleaned.get("TransactionID")
    if not txn_id:
        errors.append("Missing TransactionID")

    # Amount
    amt = cleaned.get("TransactionAmount") or cleaned.get("Amount")
    amt_val = to_float(amt)
    if amt_val is None or amt_val <= 0:
        errors.append("Invalid or non-positive Amount")
    cleaned["Amount"] = amt_val

    # Timestamp
    ts_raw = cleaned.get("TransactionTime") or cleaned.get("Timestamp")
    ts = parse_ts(ts_raw)
    if not ts:
        errors.append("Invalid Timestamp")
    cleaned["Timestamp"] = ts.isoformat() if ts else None

    return errors, cleaned
