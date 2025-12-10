# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading KYC Data from Blob

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storage4bank.dfs.core.windows.net",
    "5veT98sHoY3JAn+SMZCIVljfSp35Q3Zg2d65BCydKATr4i2UjKsHw59ERj7+GcLkgK8KLSQVV1W5+ASt+VXwpg=="
)


# COMMAND ----------

kyc_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("abfss://kyc@storage4bank.dfs.core.windows.net/")
)


# COMMAND ----------

kyc_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Loading the data into bronze container

# COMMAND ----------

kyc_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("abfss://bronze@storage4bank.dfs.core.windows.net/kyc/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cosmos Configuration

# COMMAND ----------

# MAGIC %pip install azure-cosmos

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        # Nested dict → flatten recursively
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        
        else:
            items.append((new_key, v))
    return dict(items)


# COMMAND ----------

from azure.cosmos import CosmosClient

endpoint = "https://bank-cosmos.documents.azure.com:443/"
key = "Q87iRpcVupmCsYpMSdpMuONvZK9XHvx0DnfN0WlD64MdUPRSwXajmWgmx6GBMupczVrZ2lj871zQACDbpwLnNw=="
db_name = "operation-storage-db"

SYSTEM_FIELDS = ["_rid", "_self", "_etag", "_attachments", "_ts"]

client = CosmosClient(endpoint, credential=key)

def read_cosmos(container_name, numeric_fields=[]):
    container = client.get_database_client(db_name).get_container_client(container_name)
    items = list(container.read_all_items())

    cleaned = []
    for doc in items:
        # Remove system fields
        for sf in SYSTEM_FIELDS:
            doc.pop(sf, None)

        # Flatten nested JSON
        flat = flatten_dict(doc)

        # Cast numeric fields
        for nf in numeric_fields:
            if nf in flat:
                try:
                    flat[nf] = float(flat[nf])
                except:
                    flat[nf] = None

        cleaned.append(flat)

    #return spark.createDataFrame(cleaned)
    # Convert all values to strings to avoid merge errors
    final_cleaned = []
    for row in cleaned:
        final_cleaned.append({k: str(v) if v is not None else None for k, v in row.items()})

    return spark.createDataFrame(final_cleaned)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data from Cosmos DB

# COMMAND ----------

atm_df = read_cosmos(
    "ATMTransactions",
    numeric_fields=["Amount", "Balance"]
)
display(atm_df)


# COMMAND ----------

bronze_atm=atm_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("abfss://bronze@storage4bank.dfs.core.windows.net/atm/")


# COMMAND ----------

upi_df = read_cosmos(
    "UPIEvents",
    numeric_fields=["Amount", "Balance"]
)
display(upi_df)


# COMMAND ----------

bronze_upi=upi_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("abfss://bronze@storage4bank.dfs.core.windows.net/upi/")


# COMMAND ----------

account_df = read_cosmos(
    "AccountProfile",
    numeric_fields=["Account_Balance", "Customer_AnnualIncome"]
)
display(account_df)


# COMMAND ----------

bronze_account=account_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("abfss://bronze@storage4bank.dfs.core.windows.net/account/")


# COMMAND ----------

alerts_raw = read_cosmos("FraudAlerts", numeric_fields=[])


# COMMAND ----------

from pyspark.sql.functions import col

numeric_fields = [
    "FraudScore",
    "Flags_Count",
    "Amount",
    "AbsAmount",
    "RiskScore"
]

alerts_df = alerts_raw

for f in numeric_fields:
    if f in alerts_df.columns:
        alerts_df = alerts_df.withColumn(f, col(f).cast("double"))


# COMMAND ----------

alerts_df = read_cosmos(
    "FraudAlerts",
    numeric_fields=[]
)
display(alerts_df)


# COMMAND ----------

bronze_alerts = alerts_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("abfss://bronze@storage4bank.dfs.core.windows.net/fraudalerts/")


# COMMAND ----------

