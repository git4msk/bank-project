import azure.functions as func
import logging
import os
import json
import uuid
import csv
from io import StringIO
from azure.storage.blob import BlobClient
from azure.cosmos import CosmosClient


# One shared FunctionApp for all functions
app = func.FunctionApp()


# -------- FUNCTION 1: EVENT GRID INGESTION ----------
@app.function_name(name="IngestionEventHandler")
@app.event_grid_trigger(arg_name="event")
def ingestion_event_handler(event: func.EventGridEvent):

    data = event.get_json()
    blob_url = data["url"]
    file_name = blob_url.split("/")[-1]

    logging.info(f"Event Grid detected new file: {file_name}")

    # Send message to Service Bus
    from azure.servicebus import ServiceBusClient, ServiceBusMessage

    sb_client = ServiceBusClient.from_connection_string(
        os.environ["SERVICE_BUS_CONNECTION_STRING"]
    )

    queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]

    payload = json.dumps({"file_url": blob_url, "file_name": file_name})
    with sb_client:
        sender = sb_client.get_queue_sender(queue_name)
        sender.send_messages(ServiceBusMessage(payload))

    logging.info("Event sent to Service Bus successfully.")


# -------- FUNCTION 2: SERVICE BUS FILE PROCESSOR ----------
@app.function_name(name="SBFileProcessor")
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name=os.environ["SERVICE_BUS_QUEUE_NAME"],
    connection="SERVICE_BUS_CONNECTION_STRING"
)
def sb_file_processor(msg: func.ServiceBusMessage):

    logging.info("Service Bus message received")

    body = msg.get_body().decode("utf-8")
    data = json.loads(body)

    blob_url = data["file_url"]
    file_name = data["file_name"]

    folder = blob_url.split("/raw/")[1].split("/")[0].lower()
    logging.info(f"Detected folder: {folder}")

    # Download CSV
    blob = BlobClient.from_blob_url(blob_url)
    csv_text = blob.download_blob().readall().decode("utf-8")

    reader = csv.DictReader(StringIO(csv_text))
    rows = list(reader)

    # Cosmos client
    cosmos = CosmosClient.from_connection_string(os.environ["COSMOS_CONNECTION_STRING"])
    db = cosmos.get_database_client("bankdb")

    container_map = {
        "atm": ("ATMTransactions", "ATMID"),
        "upi": ("UPIEvents", "CUSTOMERID"),
        "customers": ("AccountProfiles", "ACCOUNTNUMBER")
    }

    container_name, pk = container_map[folder]
    container = db.get_container_client(container_name)
    fraud_container = db.get_container_client("FraudAlerts")

    for row in rows:
        # Assign ID
        row["id"] = row.get(pk) or str(uuid.uuid4())

        container.upsert_item(row)

        # Fraud rule
        amount = float(row.get("amount", 0))
        if amount > 50000:
            fraud_alert = {
                "id": str(uuid.uuid4()),
                "ACCOUNTNUMBER": row.get("ACCOUNTNUMBER"),
                "amount": amount,
                "txn_type": folder,
                "reason": "High-value transaction"
            }
            fraud_container.upsert_item(fraud_alert)

    logging.info("Processing completed.")
