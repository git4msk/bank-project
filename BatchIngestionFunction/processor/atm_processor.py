# processor/atm_processor.py
import logging

from ..utils.csv_utils import parse_csv
from ..validator.transaction_validator import validate_transaction_row
from ..alerts.transaction_alerts import fraud_detection
from ..client.cosmos_client import upsert_item, upsert_items_parallel


def process_atm(text: str, file_name: str, container, alert_container):
    """
    Process ATM transaction CSV.
    Returns:
    {
        rows_parsed,
        valid,
        invalid,
        quarantined,
        alerts
    }
    """
    rows = parse_csv(text)
    total = len(rows)

    valid_rows = []
    bad_rows = []

    # ------------------------
    # 1. Validate & normalize
    # ------------------------
    for row in rows:
        errors, cleaned = validate_transaction_row(row, "ATM")
        if errors:
            bad_rows.append(row)
        else:
            valid_rows.append(cleaned)

    # ------------------------
    # 2. Upsert valid rows
    # ------------------------
    for r in valid_rows:
        # Cosmos requires id
        r["id"] = str(r.get("TransactionID"))
        upsert_item(container, r)

    # ------------------------
    # 3. Fraud alerts
    # ------------------------
    alerts = fraud_detection(valid_rows)

    for alert in alerts:
        # Normalize alert doc
        a = {
            "id": alert.get("alert_id"),
            "alert_id": alert.get("alert_id"),
            "type": alert.get("type"),
            "reason": alert.get("reason"),
            "created_at": alert.get("transaction", {}).get("Timestamp"),
            "payload": alert.get("transaction") or alert.get("transactions"),
            "AccountNumber": alert.get("transaction", {}).get("AccountNumber", "UNKNOWN")
        }
        upsert_item(alert_container, a)

    return {
        "rows_parsed": total,
        "valid": len(valid_rows),
        "invalid": len(bad_rows),
        "quarantined": len(bad_rows),
        "alerts": len(alerts)
    }
