# validator/customer_validator.py
from ..utils.sanitizer import strip, to_float
from ..utils.date_utils import parse_ts


def validate_customer_row(row: dict):
    """
    Validate customer_master row.
    Returns list of errors.
    """

    errors = []

    cust_id = strip(row.get("CustomerID"))
    if not cust_id:
        errors.append("Missing CustomerID")

    # Valid DOB
    dob = parse_ts(row.get("DOB"))
    if dob is None:
        errors.append("Invalid DOB")

    # Annual Income
    inc = to_float(row.get("AnnualIncome"))
    if inc is None:
        errors.append("Invalid AnnualIncome")

    return errors
