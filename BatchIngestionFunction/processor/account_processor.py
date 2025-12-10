# processor/account_processor.py
import logging
from typing import Dict, Any, List, Tuple

from ..utils.csv_utils import parse_csv
from ..utils.date_utils import parse_ts
from ..utils.sanitizer import strip, to_float

from ..validator.account_validator import validate_account_row
from ..alerts.profile_alerts import generate_profile_alerts
from ..client.cosmos_client import upsert_item, upsert_items_parallel

def _build_account_doc(normalized: Dict[str, Any]) -> Dict[str, Any]:
    # normalized row keys are raw strings; convert & organize into Account subdoc
    accnum = strip(normalized.get("AccountNumber"))
    custid = strip(normalized.get("CustomerID"))

    # parse/normalize
    balance = to_float(normalized.get("Balance")) or 0.0
    open_dt = parse_ts(normalized.get("AccountOpenDate"))
    open_iso = open_dt.isoformat() if open_dt else None

    account = {
        "AccountHolderName": strip(normalized.get("AccountHolderName")),
        "BankName": strip(normalized.get("BankName")),
        "BranchName": strip(normalized.get("BranchName")),
        "IFSC_Code": strip(normalized.get("IFSC_Code")),
        "AccountType": strip(normalized.get("AccountType")),
        "AccountStatus": strip(normalized.get("AccountStatus")),
        "AccountOpenDate": open_iso,
        "Balance": balance,
        "Currency": strip(normalized.get("Currency")),
        "KYC_Done": strip(normalized.get("KYC_Done")),
        "KYC_DocID": strip(normalized.get("KYC_DocID")),
        "KYC_DocumentVerificationStatus": strip(normalized.get("KYC_DocumentVerificationStatus")),
    }

    doc = {
        "id": str(accnum),
        "AccountNumber": accnum,
        "CustomerID": custid,
        "Account": account
    }
    return doc

def process_account_profiles(text: str, file_name: str, profile_container, alert_container) -> Dict[str, Any]:
    """
    Process account_master CSV content, upsert account docs into PROFILE container and create profile alerts.
    Returns metadata dict with rows_parsed, valid, invalid, quarantined, alerts.
    """
    rows = parse_csv(text)
    total = len(rows)
    if total == 0:
        return {"rows_parsed": 0, "valid": 0, "invalid": 0, "quarantined": 0, "alerts": 0}

    valid_rows = []
    bad_rows = []
    header = list(rows[0].keys()) if rows else []

    # Validate rows
    for i, raw in enumerate(rows, start=1):
        errors = validate_account_row(raw)
        if errors:
            bad_rows.append([raw.get(h, "") for h in header])
            logging.warning(f"Account row {i} invalid: {errors}")
        else:
            valid_rows.append(raw)

    # Quarantine bad rows (if any) — caller should write quarantine blob
    quarantined = len(bad_rows)

    docs = [_build_account_doc(r) for r in valid_rows]

    # Build a customer map to fetch customer details (AnnualIncome) needed for BALANCE_INCOME_MISMATCH
    customer_map = {}
    customer_ids = sorted({d["CustomerID"] for d in docs if d.get("CustomerID")})
    for cid in customer_ids:
        try:
            query = "SELECT * FROM c WHERE c.CustomerID=@cid"
            params = [{"name": "@cid", "value": cid}]
            items = list(profile_container.query_items(query=query, parameters=params, enable_cross_partition_query=True))
            # find a document that has a 'Customer' subdoc or top-level income
            cust_doc = None
            for it in items:
                if it.get("Customer"):
                    cust_doc = it.get("Customer")
                    break
            if not cust_doc and items:
                # fallback: attempt to construct customer doc from top-level fields
                candidate = items[0]
                cust_doc = {k: v for k, v in candidate.items() if k not in ("id", "Account", "AccountNumber")}
            if cust_doc:
                customer_map[cid] = cust_doc
        except Exception as e:
            logging.warning(f"Failed to query customer data for CustomerID={cid}: {e}")

    # Generate alerts for each doc (account-only path)
    all_alerts = []
    for doc in docs:
        cust_doc = customer_map.get(doc.get("CustomerID"))
        alerts = generate_profile_alerts(doc, customer_doc=cust_doc)
        all_alerts.extend(alerts)

    # Persist alerts first (best-effort)
    alerts_written = 0
    for a in all_alerts:
        try:
            upsert_item(alert_container, a)
            alerts_written += 1
        except Exception as e:
            logging.error(f"Failed to persist profile alert {a.get('id')}: {e}")

    # Upsert account docs in parallel
    ingested = 0
    failed = 0
    if docs:
        try:
            s, f = upsert_items_parallel(profile_container, docs)
            ingested += s
            failed += f
        except Exception as e:
            logging.error(f"Failed to upsert account docs: {e}")
            # conservative counts
            failed += len(docs)

    return {
        "rows_parsed": total,
        "valid": len(docs),
        "invalid": len(bad_rows),
        "quarantined": quarantined,
        "alerts": alerts_written
    }
