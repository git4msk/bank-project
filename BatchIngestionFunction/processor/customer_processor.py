# processor/customer_processor.py
import logging
from typing import Dict, Any, List

from ..utils.csv_utils import parse_csv
from ..utils.date_utils import parse_ts
from ..utils.sanitizer import strip, to_float

from ..validator.customer_validator import validate_customer_row
from ..client.cosmos_client import upsert_item, upsert_items_parallel

def _build_customer_subdoc(row: Dict[str, Any]) -> Dict[str, Any]:
    # Normalize and return the Customer subdocument
    cust = {
        "CustomerID": strip(row.get("CustomerID")),
        "FirstName": strip(row.get("FirstName")),
        "LastName": strip(row.get("LastName")),
        "DOB": None,
        "Gender": strip(row.get("Gender")),
        "Email": strip(row.get("Email")),
        "Phone": strip(row.get("Phone")),
        "Address": strip(row.get("Address")),
        "City": strip(row.get("City")),
        "State": strip(row.get("State")),
        "ZipCode": strip(row.get("ZipCode")),
        "KYC_Status": strip(row.get("KYC_Status")),
        "KYC_Tier": strip(row.get("KYC_Tier")),
        "Occupation": strip(row.get("Occupation")),
        "AnnualIncome": None
    }

    dob_dt = parse_ts(row.get("DOB"))
    if dob_dt:
        cust["DOB"] = dob_dt.isoformat()

    ai = to_float(row.get("AnnualIncome"))
    cust["AnnualIncome"] = ai if ai is not None else 0.0

    return cust

def process_customer_profiles(text: str, file_name: str, profile_container) -> Dict[str, Any]:
    """
    Process customer_master CSV content, merge into existing profile docs by CustomerID.
    If no account doc exists for a CustomerID, create a CUSTOMER_{CustomerID} doc.
    Returns metadata dict.
    """
    rows = parse_csv(text)
    total = len(rows)
    if total == 0:
        return {"rows_parsed": 0, "valid": 0, "invalid": 0, "quarantined": 0, "alerts": 0}

    valid_customers = []
    bad_rows = []
    header = list(rows[0].keys()) if rows else []

    for i, raw in enumerate(rows, start=1):
        errors = validate_customer_row(raw)
        if errors:
            bad_rows.append([raw.get(h, "") for h in header])
            logging.warning(f"Customer row {i} invalid: {errors}")
        else:
            valid_customers.append(raw)

    quarantined = len(bad_rows)

    success = 0
    failed = 0

    # For each valid customer row, build subdoc and merge
    for raw in valid_customers:
        cid = strip(raw.get("CustomerID"))
        if not cid:
            failed += 1
            continue

        customer_subdoc = _build_customer_subdoc(raw)

        # Query profile docs with this CustomerID
        try:
            query = "SELECT * FROM c WHERE c.CustomerID=@cid"
            params = [{"name": "@cid", "value": cid}]
            items = list(profile_container.query_items(query=query, parameters=params, enable_cross_partition_query=True))
        except Exception as e:
            logging.error(f"Failed to query profile docs for CustomerID={cid}: {e}")
            items = []

        if items:
            # Update each matched doc's Customer property and upsert
            updated_docs = []
            for doc in items:
                doc["Customer"] = customer_subdoc
                if not doc.get("id"):
                    doc["id"] = doc.get("AccountNumber") or f"CUSTOMER_{cid}"
                doc["CustomerID"] = cid
                updated_docs.append(doc)
            try:
                s, f = upsert_items_parallel(profile_container, updated_docs)
                success += s
                failed += f
            except Exception as e:
                logging.error(f"Failed to upsert merged customer docs for CustomerID={cid}: {e}")
                failed += len(updated_docs)
        else:
            # No account docs exist; create a standalone customer doc
            cust_doc = {
                "id": f"CUSTOMER_{cid}",
                "CustomerID": cid,
                "Customer": customer_subdoc
            }
            try:
                upsert_item(profile_container, cust_doc)
                success += 1
            except Exception as e:
                logging.error(f"Failed to upsert standalone customer doc for CustomerID={cid}: {e}")
                failed += 1

    return {
        "rows_parsed": total,
        "valid": len(valid_customers),
        "invalid": len(bad_rows),
        "quarantined": quarantined,
        "alerts": 0  # customer ingestion doesn't create profile alerts in Option A
    }
