# Banking Data Engineering Project – Azure Platform



## Overview

This project implements a cloud-based data ingestion and processing pipeline for banking datasets using Azure services. The system automatically processes files uploaded to ADLS Gen2, triggers an Event Grid notification, executes an Azure Function, and sends structured messages to an Azure Service Bus queue.

---

## Architecture Summary

### 1. Storage (ADLS Gen2)

Raw ingestion zones were created for each data domain:

* `/raw/atm/`
* `/raw/upi/`
* `/raw/customers/`

These directories act as landing zones for incoming banking data files.

### 2. Event Grid

Event Grid is configured to trigger whenever a new file is uploaded to any of the raw folders.
This event message is forwarded to the Azure Function.

### 3. Azure Function App (Python v2)

A Python Azure Function App was deployed using the v2 programming model (decorators).
The function performs the following actions:

* Receives the Event Grid payload
* Extracts the blob URL and filename
* Constructs a message with the relevant metadata
* Publishes the message to a Service Bus queue

All deployment steps completed successfully, and Azure recognizes the function without issues.

### 4. Service Bus

A Service Bus Namespace and Queue were deployed.
This acts as the downstream component that receives metadata about every new file arriving in ADLS.

---

## Deployment Status

All infrastructure components—ADLS Gen2 storage account, Event Grid setup, Azure Function App, and Service Bus—were deployed successfully as indicated in the deployment summaries.
The Function App is active, and the Service Bus queue is ready to receive messages.

---

## Workflow Summary

1. A file is uploaded into ADLS Gen2 under `/raw/atm/`, `/raw/upi/`, or `/raw/customers/`.
2. Event Grid detects the upload and triggers the Azure Function.
3. The Function extracts blob metadata and publishes a structured message to the Service Bus queue.
4. Messages appear in Service Bus whenever new files are uploaded, confirming that ingestion is working end-to-end.

This establishes an automated and event-driven ingestion pipeline for banking data.

---

## Final Outcome

The system now supports:

* Automated data ingestion
* Event-driven processing
* Reliable messaging via Service Bus

Uploading any file into ADLS immediately results in a corresponding message being generated and delivered to the queue—confirming a successful pipeline implementation.

