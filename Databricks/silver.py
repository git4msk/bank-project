# Databricks notebook source
# MAGIC %md
# MAGIC ### Blob Configuration

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storage4bank.dfs.core.windows.net",
    "5veT98sHoY3JAn+SMZCIVljfSp35Q3Zg2d65BCydKATr4i2UjKsHw59ERj7+GcLkgK8KLSQVV1W5+ASt+VXwpg=="
)


# COMMAND ----------

dbutils.fs.ls("abfss://bronze@storage4bank.dfs.core.windows.net/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Accounts Transformation

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date, months_between, floor

# ---------- Read bronze CSV for accounts ----------
bronze_account_path = "abfss://bronze@storage4bank.dfs.core.windows.net/account/"

account_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(bronze_account_path)
)

# ---------- Start transformation (silver_account) ----------
silver_account = account_df

# 1. Rename columns (your naming conventions)
rename_map = {
    "Account_AccountHolderName": "AccountHolderName",
    "Account_AccountOpenDate": "AccountOpenDate",
    "Account_AccountStatus": "AccountStatus",
    "Account_AccountType": "AccountType",
    "Account_Balance": "Balance",
    "Account_BankName": "BankName",
}

for old, new in rename_map.items():
    if old in silver_account.columns:
        silver_account = silver_account.withColumnRenamed(old, new)

# 2. Fix AccountOpenDate to proper date type
if "AccountOpenDate" in silver_account.columns:
    # if your CSV uses a specific format, pass format string to to_date, e.g. to_date(col(...), "dd-MM-yyyy")
    silver_account = silver_account.withColumn(
        "AccountOpenDate",
        to_date(col("AccountOpenDate"))
    )

# 3. Fix Customer DOB and calculate age
if "Customer_DOB" in silver_account.columns:
    silver_account = (
        silver_account
        .withColumn("Customer_DOB", to_date(col("Customer_DOB")))
        .withColumn(
            "Customer_Age",
            floor(months_between(current_date(), col("Customer_DOB")) / 12)
        )
    )

# 4. Drop unwanted column
if "Customer_CustomerID" in silver_account.columns:
    silver_account = silver_account.drop("Customer_CustomerID")

# 5. Optional: cast Balance to double (if present)
if "Balance" in silver_account.columns:
    silver_account = silver_account.withColumn("Balance", col("Balance").cast("double"))

# 6. Remove duplicates (optional)
silver_account = silver_account.dropDuplicates()

# 7. Show transformed result
display(silver_account.limit(50))

# ---------- Write to Silver (Parquet recommended) ----------
silver_path = "abfss://silver@storage4bank.dfs.core.windows.net/accounts/"

silver_account.write.mode("overwrite").parquet(silver_path)



# COMMAND ----------

# MAGIC %md
# MAGIC ### ATM Transformation

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, to_date, date_format
)

# ------------------ READ BRONZE ATM ------------------
bronze_atm_path = "abfss://bronze@storage4bank.dfs.core.windows.net/atm/"

atm_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(bronze_atm_path)
)

# ------------------ START SILVER TRANSFORMATION ------------------
silver_atm = atm_df

# 1. Rename ONLY TransactionTime (real transaction timestamp)
if "TransactionTime" in silver_atm.columns:
    silver_atm = silver_atm.withColumnRenamed("TransactionTime", "Transaction_Timestamp")

# -----------------------------------------------------------
# 2. Extract date & time from Transaction_Timestamp
# -----------------------------------------------------------

silver_atm = silver_atm.withColumn(
    "Transaction_TS",
    to_timestamp(col("Transaction_Timestamp"))
)

silver_atm = silver_atm.withColumn(
    "Transaction_Date",
    to_date(col("Transaction_TS"))
)

silver_atm = silver_atm.withColumn(
    "Transaction_Time",
    date_format(col("Transaction_TS"), "HH:mm:ss")
)

# -----------------------------------------------------------
# 3. DROP BOTH unnecessary timestamp columns
# -----------------------------------------------------------
silver_atm = silver_atm.drop("Transaction_Timestamp", "Timestamp", "Transaction_TS")

# -----------------------------------------------------------
# 4. Rename other fields
rename_map_atm = {
    "ATMID": "ATM_ID",
    "ATM_Bank": "ATM_BankName",
    "TransactionType": "Transaction_Type",
    "TransactionStatus": "Transaction_Status"
}

for old, new in rename_map_atm.items():
    if old in silver_atm.columns:
        silver_atm = silver_atm.withColumnRenamed(old, new)

# -----------------------------------------------------------
# 5. Cast numeric fields
numeric_cols = ["Amount", "BalanceAfter", "BalanceBefore"]

for c in numeric_cols:
    if c in silver_atm.columns:
        silver_atm = silver_atm.withColumn(c, col(c).cast("double"))

# -----------------------------------------------------------
# 6. Calculate BalanceChange
if all(c in silver_atm.columns for c in ["BalanceAfter", "BalanceBefore"]):
    silver_atm = silver_atm.withColumn(
        "BalanceChange",
        col("BalanceAfter") - col("BalanceBefore")
    )

# -----------------------------------------------------------
# 7. Remove duplicates
silver_atm = silver_atm.dropDuplicates()

# Preview
display(silver_atm.limit(50))

# ------------------ WRITE TO SILVER ------------------
silver_atm_path = "abfss://silver@storage4bank.dfs.core.windows.net/atm/"

silver_atm.write.mode("overwrite").parquet(silver_atm_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### UPI Transformation

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, to_date, date_format, split
)

# ------------------ READ BRONZE UPI ------------------
bronze_upi_path = "abfss://bronze@storage4bank.dfs.core.windows.net/upi/"

upi_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(bronze_upi_path)
)

# ------------------ START SILVER TRANSFORMATION ------------------
silver_upi = upi_df

# -----------------------------------------------------------
# 1. Standardize TransactionTime → Date + Time
# -----------------------------------------------------------

# Convert raw string → timestamp
silver_upi = silver_upi.withColumn(
    "Transaction_TS",
    to_timestamp(col("TransactionTime"))
)

# Extract only date
silver_upi = silver_upi.withColumn(
    "Transaction_Date",
    to_date(col("Transaction_TS"))
)

# Extract only clock time (HH:mm:ss)
silver_upi = silver_upi.withColumn(
    "Transaction_Time",
    date_format(col("Transaction_TS"), "HH:mm:ss")
)

# -----------------------------------------------------------
# 2. Split GeoLocation "lat,long" → separate fields
# -----------------------------------------------------------
if "GeoLocation" in silver_upi.columns:
    silver_upi = (
        silver_upi
        .withColumn("Geo_Lat",  split(col("GeoLocation"), ",").getItem(0))
        .withColumn("Geo_Long", split(col("GeoLocation"), ",").getItem(1))
        .drop("GeoLocation")
    )

# -----------------------------------------------------------
# 3. Cast numeric fields (recommended for Silver layer)
# -----------------------------------------------------------
numeric_cols = ["Amount", "BalanceBefore", "BalanceAfter"]

for c in numeric_cols:
    if c in silver_upi.columns:
        silver_upi = silver_upi.withColumn(c, col(c).cast("double"))

# -----------------------------------------------------------
# 4. Drop raw fields not needed in Silver
# -----------------------------------------------------------
drop_cols = ["Timestamp", "TransactionTime", "Transaction_TS"]

for c in drop_cols:
    if c in silver_upi.columns:
        silver_upi = silver_upi.drop(c)

# -----------------------------------------------------------
# 5. Remove duplicates
# -----------------------------------------------------------
silver_upi = silver_upi.dropDuplicates()

# -----------------------------------------------------------
# 6. Display result
# -----------------------------------------------------------
display(silver_upi.limit(50))

# ------------------ WRITE TO SILVER ------------------
silver_upi_path = "abfss://silver@storage4bank.dfs.core.windows.net/upi/"

silver_upi.write.mode("overwrite").parquet(silver_upi_path)




# COMMAND ----------

# MAGIC %md
# MAGIC ### Fraud Transformation

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, current_date, when, lower
)

# ------------------ READ BRONZE FRAUD ALERTS ------------------
bronze_fraud_path = "abfss://bronze@storage4bank.dfs.core.windows.net/fraudalerts/"

alerts_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(bronze_fraud_path)
)

# ------------------ START SILVER TRANSFORMATION ------------------
silver_fraud = alerts_df

# -----------------------------------------------------------
# 1. RENAME COLUMNS (remove payload_ prefix)
# -----------------------------------------------------------

rename_map_fraud = {
    "payload_AccountHolderName": "AccountHolderName",
    "payload_AccountOpenDate": "AccountOpenDate",
    "payload_AccountStatus": "AccountStatus",
    "payload_AccountType": "AccountType",
    "payload_Balance": "Balance",
    "payload_BankName": "BankName",
    "payload_BranchName": "BranchName",
    "payload_Currency": "Currency",
    "payload_IFSC_Code": "IFSC_Code",
    "payload_KYC_DocID": "KYC_DocID",
    "payload_KYC_DocumentVerificationStatus": "KYC_VerificationStatus",
    "payload_KYC_Done": "KYC_Done"
}

for old, new in rename_map_fraud.items():
    if old in silver_fraud.columns:
        silver_fraud = silver_fraud.withColumnRenamed(old, new)

# -----------------------------------------------------------
# 2. FIX DATE FORMAT
# -----------------------------------------------------------
if "AccountOpenDate" in silver_fraud.columns:
    silver_fraud = silver_fraud.withColumn(
        "AccountOpenDate",
        to_date(col("AccountOpenDate"))
    )

# -----------------------------------------------------------
# 3. NORMALIZE KYC STATUS → boolean
# -----------------------------------------------------------
if "KYC_Done" in silver_fraud.columns:
    silver_fraud = silver_fraud.withColumn(
        "KYC_Done",
        when(lower(col("KYC_Done")) == "verified", True)
        .when(lower(col("KYC_Done")) == "true", True)
        .otherwise(False)
    )

# -----------------------------------------------------------
# 4. CLASSIFY ALERT SEVERITY
# -----------------------------------------------------------
silver_fraud = silver_fraud.withColumn(
    "AlertSeverity",
    when(lower(col("reason")).contains("mismatch"), "HIGH")
    .when(lower(col("reason")).contains("unverified"), "MEDIUM")
    .otherwise("LOW")
)

# -----------------------------------------------------------
# 5. ADD ALERT DATE
# -----------------------------------------------------------
silver_fraud = silver_fraud.withColumn("AlertDate", current_date())

# -----------------------------------------------------------
# 6. DROP UNUSED IDENTIFIERS
# -----------------------------------------------------------
if "id" in silver_fraud.columns:
    silver_fraud = silver_fraud.drop("id")

# -----------------------------------------------------------
# 7. REMOVE DUPLICATES
# -----------------------------------------------------------
silver_fraud = silver_fraud.dropDuplicates()

# -----------------------------------------------------------
# 8. DISPLAY
# -----------------------------------------------------------
display(silver_fraud.limit(50))

# ------------------ WRITE TO SILVER ------------------
silver_fraud_path = "abfss://silver@storage4bank.dfs.core.windows.net/fraudalerts/"

silver_fraud.write.mode("overwrite").parquet(silver_fraud_path)



# COMMAND ----------

# MAGIC %md
# MAGIC ### KYC Transformation

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, current_date, when, months_between, floor, lower
)

# ------------------ READ BRONZE KYC ------------------
bronze_kyc_path = "abfss://bronze@storage4bank.dfs.core.windows.net/kyc/"

kyc_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(bronze_kyc_path)
)

# ------------------ START SILVER TRANSFORMATION ------------------
silver_kyc = kyc_df

# -----------------------------------------------------------
# 1. Standardize date columns
# -----------------------------------------------------------
date_cols = ["IssueDate", "ExpiryDate"]

for c in date_cols:
    if c in silver_kyc.columns:
        silver_kyc = silver_kyc.withColumn(c, to_date(col(c)))

# -----------------------------------------------------------
# 2. Document validity status
# -----------------------------------------------------------
if "ExpiryDate" in silver_kyc.columns:
    silver_kyc = silver_kyc.withColumn(
        "DocumentValidityStatus",
        when(col("ExpiryDate").isNull(), "UNKNOWN")
        .when(col("ExpiryDate") < current_date(), "EXPIRED")
        .otherwise("VALID")
    )

# -----------------------------------------------------------
# 3. Document age (years since IssueDate)
# -----------------------------------------------------------
if "IssueDate" in silver_kyc.columns:
    silver_kyc = silver_kyc.withColumn(
        "DocumentAgeYears",
        floor(months_between(current_date(), col("IssueDate")) / 12)
    )

# -----------------------------------------------------------
# 4. Normalize VerificationStatus to boolean
# -----------------------------------------------------------
if "VerificationStatus" in silver_kyc.columns:
    silver_kyc = silver_kyc.withColumn(
        "IsVerified",
        when(lower(col("VerificationStatus")).isin("verified", "true"), True)
        .otherwise(False)
    )

# -----------------------------------------------------------
# 5. Drop DocumentFile if large or unnecessary
# -----------------------------------------------------------
if "DocumentFile" in silver_kyc.columns:
    silver_kyc = silver_kyc.drop("DocumentFile")

# -----------------------------------------------------------
# 6. Remove duplicates
# -----------------------------------------------------------
silver_kyc = silver_kyc.dropDuplicates()

# Display
display(silver_kyc.limit(50))

# ------------------ WRITE TO SILVER ------------------
silver_kyc_path = "abfss://silver@storage4bank.dfs.core.windows.net/kyc/"

silver_kyc.write.mode("overwrite").parquet(silver_kyc_path)




# COMMAND ----------

# MAGIC %md
# MAGIC ### ATM + UPI
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit


# COMMAND ----------

from pyspark.sql.functions import col, lit

gold_atm = silver_atm.select(
    col("AccountNumber"),
    col("Transaction_Date"),
    col("Transaction_Time"),
    col("Amount"),
    lit("ATM").alias("Channel"),
    col("ATM_ID").alias("Location_ID"),
    col("Transaction_Type"),
    col("Transaction_Status")
)


# COMMAND ----------

gold_upi = silver_upi.select(
    col("AccountNumber"),
    col("Transaction_Date"),
    col("Transaction_Time"),
    col("Amount"),
    lit("UPI").alias("Channel"),
    col("Payer_UPI_ID").alias("Location_ID"),
    col("TransactionType").alias("Transaction_Type"),
    col("Status").alias("Transaction_Status")
)


# COMMAND ----------

gold_transactions = gold_atm.unionByName(gold_upi)
gold_transactions.display()

# COMMAND ----------

gold_transactions.write.mode("overwrite").parquet("abfss://gold@storage4bank.dfs.core.windows.net/transactions/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer 360

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_date, current_date, months_between, floor

# ---------------------------
# 0. Paths & names
# ---------------------------
gold_customer_path = "abfss://gold@storage4bank.dfs.core.windows.net/customer360/"

# ---------------------------
# 1. Prepare helper lookups
# ---------------------------
# Select minimal account info (use exact columns you provided)
accounts_small = silver_account.select(
    col("AccountNumber"),
    col("CustomerID"),
    col("AccountType"),
    col("AccountStatus"),
    col("Balance").cast("double"),
    col("AccountOpenDate")
)

# KYC aggregation per CustomerID (latest verification status & latest expiry)
kyc_agg = silver_kyc.groupBy("CustomerID").agg(
    F.max("VerificationStatus").alias("Latest_KYC_VerificationStatus"),
    F.max("ExpiryDate").alias("Latest_KYC_Expiry"),
    F.max("IsVerified").alias("Any_KYC_Verified")  # boolean if any doc verified
)

# ---------------------------
# 2. Attach CustomerID to transactions
# ---------------------------
# gold_transactions must exist (result of your union). It should include AccountNumber.
txn_with_cust = (
    gold_transactions.alias("g")
    .join(accounts_small.select("AccountNumber", "CustomerID").alias("a"),
          on="AccountNumber", how="left")
)

# ---------------------------
# 3. Transaction metrics (per AccountNumber)
# ---------------------------
# Derive common transaction flags and numeric metrics
txn_enriched = txn_with_cust.withColumn(
    "IsDebit", F.when(col("Amount") < 0, 1).otherwise(0)
).withColumn(
    "IsCredit", F.when(col("Amount") > 0, 1).otherwise(0)
).withColumn(
    "IsFailed", F.when(col("Transaction_Status").isNotNull() & (F.lower(col("Transaction_Status")) != "success"), 1).otherwise(0)
)

# Aggregation window: overall and last 12 months
txn_metrics_account = txn_enriched.groupBy("AccountNumber", "CustomerID").agg(
    F.count("*").alias("Account_Total_Transactions"),
    F.sum("IsDebit").alias("Account_Total_Debit_Count"),
    F.sum("IsCredit").alias("Account_Total_Credit_Count"),
    F.sum(F.when(col("Amount") < 0, F.abs(col("Amount"))).otherwise(0)).alias("Account_Total_Spend"),
    F.sum(F.when(col("Amount") > 0, col("Amount")).otherwise(0)).alias("Account_Total_Income"),
    F.max("Amount").alias("Account_Max_Transaction"),
    F.sum("IsFailed").alias("Account_Failed_Transactions"),
    F.max("Transaction_Date").alias("Account_Last_Transaction_Date")
)

# Also compute 12-month spend (if Transaction_Date typed as date)
txn_12m = txn_enriched.filter(
    col("Transaction_Date") >= F.add_months(current_date(), -12)
).groupBy("AccountNumber").agg(
    F.sum(F.when(col("Amount") < 0, F.abs(col("Amount"))).otherwise(0)).alias("Account_Spend_12M"),
    F.count("*").alias("Account_Txn_Count_12M")
)

# Join back 12M metrics
txn_metrics_account = txn_metrics_account.join(txn_12m, on="AccountNumber", how="left")

# ---------------------------
# 4. Roll-up per CustomerID
# ---------------------------
# Join accounts_small (for balances and account metadata) with per-account txn metrics
acct_plus_metrics = accounts_small.join(txn_metrics_account, on=["AccountNumber", "CustomerID"], how="left")

# Aggregate across accounts to customer level
customer_txn_agg = acct_plus_metrics.groupBy("CustomerID").agg(
    F.collect_set("AccountNumber").alias("Accounts"),
    F.countDistinct("AccountNumber").alias("Num_Accounts"),
    F.sum("Balance").alias("Total_Balance"),
    F.sum("Account_Total_Transactions").alias("Total_Transactions"),
    F.sum("Account_Total_Spend").alias("Total_Spend"),
    F.sum("Account_Total_Income").alias("Total_Income"),
    F.sum("Account_Failed_Transactions").alias("Failed_Transactions"),
    F.max("Account_Last_Transaction_Date").alias("Last_Transaction_Date"),
    F.max("Account_Max_Transaction").alias("Max_Transaction"),
    F.sum("Account_Spend_12M").alias("Total_Spend_12M"),
    F.sum("Account_Txn_Count_12M").alias("Txn_Count_12M")
)

# ---------------------------
# 5. Bring in customer profile attributes (from silver_account; use first occurrence per Customer)
# ---------------------------
# Build customer profile snapshot by picking first non-null value per CustomerID
cust_profile = silver_account.groupBy("CustomerID").agg(
    F.first("Customer_FirstName").alias("FirstName"),
    F.first("Customer_LastName").alias("LastName"),
    F.first("Customer_Email").alias("Email"),
    F.first("Customer_Phone").alias("Phone"),
    F.first("Customer_City").alias("City"),
    F.first("Customer_State").alias("State"),
    F.first("Customer_ZipCode").alias("ZipCode"),
    F.first("Customer_AnnualIncome").alias("AnnualIncome"),
    F.first("Customer_Age").alias("Customer_Age"),
    F.first("Customer_Occupation").alias("Occupation"),
    F.first("Customer_KYC_Tier").alias("KYC_Tier"),
    F.first("Customer_KYC_Status").alias("Customer_KYC_Status")
)

# ---------------------------
# 6. Merge everything together (profile + txn_agg + kyc)
# ---------------------------
customer360 = (
    cust_profile
    .join(customer_txn_agg, on="CustomerID", how="left")
    .join(kyc_agg, on="CustomerID", how="left")
)

# ---------------------------
# 7. Derive additional flags & simple risk score
# ---------------------------
# Is KYC expired?
customer360 = customer360.withColumn(
    "Is_KYC_Expired",
    F.when(col("Latest_KYC_Expiry").isNotNull() & (col("Latest_KYC_Expiry") < current_date()), True).otherwise(False)
)

# Recent activity (3 months)
customer360 = customer360.withColumn(
    "Is_Recent_Active",
    F.when(col("Last_Transaction_Date").isNotNull() & (col("Last_Transaction_Date") >= F.add_months(current_date(), -3)), True).otherwise(False)
)

# Simple risk score (0-100) example -- you can replace with ML scoring later
# Criteria (example weights):
# - Failed transactions (weight 0.4)
# - High average monthly spend / sudden high tx (weight 0.4)
# - Missing KYC (weight 0.2)
customer360 = customer360.withColumn(
    "RiskScore",
    (
        F.least(F.coalesce(col("Failed_Transactions"), F.lit(0)) * 2.0, F.lit(40.0))
        +
        F.least(
            F.coalesce(col("Max_Transaction"), F.lit(0)) /
            F.greatest(F.coalesce(col("Total_Spend_12M"), F.lit(1)), F.lit(1)) * 10.0,
            F.lit(40.0)
        )
        +
        F.when(col("Any_KYC_Verified") == True, F.lit(0.0)).otherwise(F.lit(20.0))
    )
)


# Normalize RiskScore to 0-100 (already approximately in that range)
customer360 = customer360.withColumn(
    "RiskScore",
    F.when(col("RiskScore") > 100, F.lit(100.0)).otherwise(col("RiskScore"))
)

# ---------------------------
# 8. Final column projection & cleanup
# ---------------------------
final_cols = [
    "CustomerID",
    "FirstName", "LastName", "Email", "Phone", "City", "State", "ZipCode",
    "Occupation", "AnnualIncome", "Customer_Age", "KYC_Tier", "Customer_KYC_Status",
    "Latest_KYC_VerificationStatus", "Latest_KYC_Expiry", "Is_KYC_Expired", "Any_KYC_Verified",
    "Accounts", "Num_Accounts", "Total_Balance",
    "Total_Transactions", "Total_Spend", "Total_Income", "Total_Spend_12M", "Txn_Count_12M",
    "Failed_Transactions", "Last_Transaction_Date", "Max_Transaction",
    "Is_Recent_Active", "RiskScore"
]

customer360_final = customer360.select(*[c for c in final_cols if c in customer360.columns])

# ---------------------------
# 9. Write GOLD Customer360
# ---------------------------
customer360_final.write.mode("overwrite").parquet(gold_customer_path)

# Show preview
display(customer360_final.limit(50))


# COMMAND ----------

customer360_final.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -----------------------------
# Simple Fraud Rules That Always Return HIGH Alerts
# -----------------------------

HIGH_AMOUNT_THRESHOLD = 10000   # guarantee hits
FAILED_STATUS_LIST = ["failed", "declined", "rejected"]

txns = gold_transactions.withColumn(
    "Transaction_TS",
    F.to_timestamp(F.concat_ws(" ", "Transaction_Date", "Transaction_Time"))
)

# -----------------------------
# Rules
# -----------------------------

txns = txns.withColumn("AbsAmount", F.abs(F.col("Amount")))

# High amount rule
txns = txns.withColumn(
    "FLAG_HIGH_AMOUNT",
    F.when(F.col("AbsAmount") > HIGH_AMOUNT_THRESHOLD, 1).otherwise(0)
)

# Failed / non-success status
txns = txns.withColumn(
    "FLAG_STATUS_FAIL",
    F.when(F.lower(F.col("Transaction_Status")).isin(*FAILED_STATUS_LIST), 1).otherwise(0)
)

# ATM specific flag
txns = txns.withColumn(
    "FLAG_ATM_LARGE_WITHDRAW",
    F.when((F.col("Channel") == "ATM") & (F.col("AbsAmount") > 5000), 1).otherwise(0)
)

# UPI anomaly flag (example)
txns = txns.withColumn(
    "FLAG_UPI_ANOMALY",
    F.when((F.col("Channel") == "UPI") & (F.col("Amount") > 8000), 1).otherwise(0)
)

# -----------------------------
# Force alerts for every 3rd transaction (guaranteed 30–40%)
# -----------------------------
w = Window.orderBy("AccountNumber", "Transaction_TS")

txns = txns.withColumn(
    "RowNum",
    F.row_number().over(w)
)

txns = txns.withColumn(
    "FLAG_FORCE",
    F.when((F.col("RowNum") % 3 == 0), 1).otherwise(0)
)

# -----------------------------
# Combine flags
# -----------------------------
txns = txns.withColumn(
    "Flags_Count",
    F.col("FLAG_HIGH_AMOUNT")
    + F.col("FLAG_STATUS_FAIL")
    + F.col("FLAG_ATM_LARGE_WITHDRAW")
    + F.col("FLAG_UPI_ANOMALY")
    + F.col("FLAG_FORCE")
)

# -----------------------------
# Final Severity
# -----------------------------
txns = txns.withColumn(
    "FraudSeverity",
    F.when(F.col("Flags_Count") > 0, "HIGH").otherwise("LOW")
)

# -----------------------------
# Build output
# -----------------------------
fraud_alerts = txns.filter(F.col("FraudSeverity") == "HIGH") \
                   .withColumn("AlertID", F.concat_ws("_", "AccountNumber", "Transaction_TS")) \
                   .withColumn("AlertDate", F.current_timestamp())

display(fraud_alerts.limit(50))


# COMMAND ----------

