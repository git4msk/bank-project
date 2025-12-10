import json
import logging
import os
import azure.functions as func
from datetime import datetime, timezone
import csv
import io

# ------------------------------
# Import our new modular files
# ------------------------------
from .client.blob_client import read_blob_text
from .client.cosmos_client import get_or_create_container

from .utils.csv_utils import detect_source_type
from .utils.date_utils import utcnow

from .processor.atm_processor import process_atm
from .processor.upi_processor import process_upi
from .processor.account_processor import process_account_profiles
from .processor.customer_processor import process_customer_profiles


# -------------------------------------------------------------------
# Containers created ONCE (Option A — centralized creation)
# -------------------------------------------------------------------
ATM_CONTAINER_NAME = os.environ.get("ATM_CONTAINER", "ATMTransactions")
UPI_CONTAINER_NAME = os.environ.get("UPI_CONTAINER", "UPIEvents")
PROFILE_CONTAINER_NAME = os.environ.get("PROFILE_CONTAINER", "AccountProfile")
ALERT_CONTAINER_NAME = os.environ.get("ALERTS_CONTAINER", "FraudAlerts")
METADATA_CONTAINER = os.environ.get("METADATA_CONTAINER", "metadata")
QUARANTINE_CONTAINER = os.environ.get("QUARANTINE_CONTAINER", "quarantine")

# Cosmos DB containers
atm_container = get_or_create_container(ATM_CONTAINER_NAME, "/AccountNumber")
upi_container = get_or_create_container(UPI_CONTAINER_NAME, "/AccountNumber")
profile_container = get_or_create_container(PROFILE_CONTAINER_NAME, "/CustomerID")
alert_container = get_or_create_container(ALERT_CONTAINER_NAME, "/AccountNumber")


# -------------------------------------------------------------------
# Helper: write metadata JSON to blob storage
# -------------------------------------------------------------------
from azure.storage.blob import BlobServiceClient
_STORAGE_CONN = os.environ.get("STORAGE_CONNECTION_STRING")
_blob_client = BlobServiceClient.from_connection_string(_STORAGE_CONN)

def write_metadata_blob(file_name: str, metadata: dict):
    container_client = _blob_client.get_container_client(METADATA_CONTAINER)
    try:
        container_client.create_container()
    except:
        pass

    blob = container_client.get_blob_client(f"{file_name}.metadata.json")
    blob.upload_blob(json.dumps(metadata, indent=2), overwrite=True)


# -------------------------------------------------------------------
# Helper: write quarantine CSV
# -------------------------------------------------------------------
def write_quarantine_blob(base_file_name: str, header, bad_rows):
    if not bad_rows:
        return
    container_client = _blob_client.get_container_client(QUARANTINE_CONTAINER)
    try:
        container_client.create_container()
    except:
        pass

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(bad_rows)

    blob_name = f"{base_file_name}_badrows.csv"
    blob = container_client.get_blob_client(blob_name)
    blob.upload_blob(buf.getvalue(), overwrite=True)


# -------------------------------------------------------------------
# MAIN ENTRY FUNCTION
# -------------------------------------------------------------------
def main(msg: func.ServiceBusMessage):
    logging.info("BatchIngestionFunction triggered.")

    # -----------------------------
    # Read service bus message
    # -----------------------------
    try:
        body = json.loads(msg.get_body().decode("utf-8"))
    except Exception as e:
        logging.error(f"Invalid message: {e}")
        return

    file_url = body.get("file_url")
    file_name = body.get("file_name")

    if not file_url or not file_name:
        logging.error("Missing file_url or file_name in message")
        return

    # Identify source type
    source_type = detect_source_type(file_name)
    logging.info(f"Processing {file_name}, source_type={source_type}")

    # Initial metadata
    metadata = {
        "file_name": file_name,
        "file_url": file_url,
        "source_type": source_type,
        "status": "PROCESSING",
        "started_at": utcnow().isoformat()
    }
    write_metadata_blob(file_name, metadata)

    # -----------------------------
    # Download blob CSV content
    # -----------------------------
    try:
        text = read_blob_text(file_url)
    except Exception as e:
        metadata["status"] = "DOWNLOAD_FAILED"
        metadata["error"] = str(e)
        write_metadata_blob(file_name, metadata)
        logging.error(f"Blob read failed: {e}")
        return

    # -----------------------------
    # DISPATCH TO PROCESSOR
    # -----------------------------
    result = {}

    try:
        if source_type == "ATM":
            result = process_atm(text, file_name, atm_container, alert_container)

        elif source_type == "UPI":
            result = process_upi(text, file_name, upi_container, alert_container)

        elif source_type == "ACCOUNT":
            result = process_account_profiles(text, file_name, profile_container, alert_container)

        elif source_type == "CUSTOMER":
            result = process_customer_profiles(text, file_name, profile_container)

        else:
            logging.error(f"Unknown source_type for file: {file_name}")
            metadata["status"] = "UNKNOWN_SOURCE"
            write_metadata_blob(file_name, metadata)
            return

    except Exception as e:
        logging.error(f"Processor exception: {e}")
        metadata["status"] = "PROCESSOR_FAILED"
        metadata["error"] = str(e)
        write_metadata_blob(file_name, metadata)
        return

    # ---------------------------------
    # Handle quarantine (if any)
    # ---------------------------------
    if result.get("invalid", 0) > 0:
        # We need header + bad rows — processors do not store them.
        # Instead, re-parse CSV here and collect invalid rows
        rows = text.splitlines()
        header = rows[0].split(",")
        # A simple fallback; actual processors could return bad_rows if needed.

    # ---------------------------------
    # Finalize metadata
    # ---------------------------------
    metadata.update({
        "status": "COMPLETED",
        "completed_at": utcnow().isoformat(),
        "rows_parsed": result.get("rows_parsed", 0),
        "valid": result.get("valid", 0),
        "invalid": result.get("invalid", 0),
        "quarantined": result.get("quarantined", 0),
        "alerts_generated": result.get("alerts", 0)
    })

    write_metadata_blob(file_name, metadata)

    logging.info(f"Completed processing {file_name} → {json.dumps(metadata)}")
