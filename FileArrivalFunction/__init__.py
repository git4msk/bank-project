import json
import logging
import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import os

def main(event: func.EventGridEvent):

    logging.info("Event Grid Trigger Fired for New File Upload")

    event_data = event.get_json()
    url = event_data["url"]                         # file URL
    file_name = url.split("/")[-1]                 # extract filename

    logging.info(f"File Uploaded: {file_name}")

    # Basic Validation
    if not file_name.endswith(".csv"):
        logging.error("Invalid file format. Only CSV allowed.")
        return

    # Detect source type based on filename
    source_type = ""
    if "atm" in file_name.lower():
        source_type = "ATM"
    elif "upi" in file_name.lower():
        source_type = "UPI"
    elif "customer" in file_name.lower():
        source_type = "CUSTOMER"
    elif "account" in file_name.lower():
        source_type = "ACCOUNT"
    else:
        source_type = "UNKNOWN"

    # Prepare message body
    message_body = {
        "file_url": url,
        "file_name": file_name,
        "source_type": source_type
    }

    logging.info(f"Sending message to Service Bus: {message_body}")

    # Send to Service Bus Queue
    sb_client = ServiceBusClient.from_connection_string(
        conn_str=os.environ["SERVICE_BUS_CONNECTION_STRING"]
    )

    queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]

    with sb_client:
        sender = sb_client.get_queue_sender(queue_name=queue_name)
        with sender:
            sb_message = ServiceBusMessage(json.dumps(message_body))
            sender.send_messages(sb_message)

    logging.info("Message sent to Service Bus Queue successfully.")
