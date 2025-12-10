# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.storage4bank.dfs.core.windows.net",
    "5veT98sHoY3JAn+SMZCIVljfSp35Q3Zg2d65BCydKATr4i2UjKsHw59ERj7+GcLkgK8KLSQVV1W5+ASt+VXwpg=="
)


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG msk_1836173769360033;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bank_dw;
# MAGIC USE SCHEMA bank_dw;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;
# MAGIC

# COMMAND ----------

df_c.printSchema()
df_t.printSchema()
df_f.printSchema()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

gold_path = "abfss://gold@storage4bank.dfs.core.windows.net/"

dim_customer_path = f"{gold_path}dim_customer"
customer360_path = f"{gold_path}customer360"

df_c = spark.read.format("parquet").load(customer360_path)

compare_cols = [
    "FirstName","LastName","Email","Phone","City","State","ZipCode",
    "Occupation","AnnualIncome","Customer_Age",
    "KYC_Tier","Customer_KYC_Status","Latest_KYC_VerificationStatus",
    "Latest_KYC_Expiry","Is_KYC_Expired","Any_KYC_Verified",
    "Total_Balance","Num_Accounts","Total_Transactions","RiskScore"
]


# COMMAND ----------

src = df_c.select(
    "CustomerID",
    *compare_cols,
    F.col("Last_Transaction_Date").alias("LastTransactionDate"),
    F.current_timestamp().alias("EffectiveFrom")
).withColumn("EffectiveTo", F.lit(None).cast("timestamp")) \
 .withColumn("CurrentFlag", F.lit(True))


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, dim_customer_path):
    
    df_init = src.withColumn(
        "CustomerSK", F.monotonically_increasing_id().cast("bigint")
    )

    df_init = df_init.select(
        "CustomerSK","CustomerID",
        *compare_cols,
        "LastTransactionDate","EffectiveFrom","EffectiveTo","CurrentFlag"
    )

    df_init.write.format("delta").mode("overwrite").save(dim_customer_path)

    print("Initial dim_customer created")

else:
    print("dim_customer already exists – proceeding to SCD2 merge")


# COMMAND ----------

dim = DeltaTable.forPath(spark, dim_customer_path)

change_expr = " OR ".join([
    f"(t.{c} <> s.{c} OR (t.{c} IS NULL AND s.{c} IS NOT NULL) OR (t.{c} IS NOT NULL AND s.{c} IS NULL))"
    for c in compare_cols + ["LastTransactionDate"]
])

dim.alias("t").merge(
    src.alias("s"),
    "t.CustomerID = s.CustomerID AND t.CurrentFlag = true"
).whenMatchedUpdate(
    condition = change_expr,
    set = {
        "CurrentFlag": "false",
        "EffectiveTo": "s.EffectiveFrom"
    }
).execute()

print("Expired changed rows")


# COMMAND ----------

tgt_current = spark.read.format("delta").load(dim_customer_path) \
    .filter("CurrentFlag = true") \
    .select("CustomerID")

to_insert = src.join(tgt_current, "CustomerID", "left_anti")

print("Rows to insert:", to_insert.count())

if to_insert.count() > 0:

    max_sk = spark.read.format("delta").load(dim_customer_path) \
        .agg(F.max("CustomerSK")).first()[0]

    if max_sk is None:
        max_sk = 0

    w = Window.orderBy("CustomerID")

    to_insert = to_insert.withColumn(
        "CustomerSK",
        (F.row_number().over(w) + max_sk).cast("bigint")
    )

    cols = ["CustomerSK","CustomerID"] + compare_cols + \
        ["LastTransactionDate","EffectiveFrom","EffectiveTo","CurrentFlag"]

    to_insert.select(*cols).write.format("delta").mode("append").save(dim_customer_path)

    print("Inserted new SCD2 records")

else:
    print("No new customer rows to insert")


# COMMAND ----------

dim_cus_df = spark.read.format("delta").load(dim_customer_path)

print("Total rows:", dim_cus_df.count())
print("Active rows:", dim_cus_df.filter("CurrentFlag = true").count())

dim_cus_df.orderBy("CustomerID","EffectiveFrom").display(50, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM ACC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

silver_path = "abfss://silver@storage4bank.dfs.core.windows.net/"
gold_path = "abfss://gold@storage4bank.dfs.core.windows.net/"

silver_account_path = f"{silver_path}account"
dim_account_path = f"{gold_path}dim_account"

df_acc = spark.read.format("parquet").load(silver_account_path)

df_acc.printSchema()
df_acc.display(5)


# COMMAND ----------

df_acct = df_acc.select(
    F.col("AccountNumber").cast("string"),
    "AccountOpenDate",
    "AccountStatus",
    "AccountType",
    "Balance",
    "BankName",
    F.col("Account_BranchName").alias("BranchName"),
    F.col("Account_Currency").alias("Currency"),
    F.col("Account_IFSC_Code").alias("IFSC_Code"),
    F.col("Account_KYC_Done").alias("Is_KYC_Done")
).dropDuplicates(["AccountNumber"])


# COMMAND ----------

df_init = df_acct.withColumn(
    "AccountSK", F.monotonically_increasing_id().cast("bigint")
)

df_init = df_init.select(
    "AccountSK",
    "AccountNumber",
    "AccountOpenDate",
    "AccountStatus",
    "AccountType",
    "Balance",
    "BankName",
    "BranchName",
    "Currency",
    "IFSC_Code",
    "Is_KYC_Done"
)

df_init.write.format("delta").mode("overwrite").save(dim_account_path)

print("DimAccount CREATED successfully")


# COMMAND ----------

dim_acc = DeltaTable.forPath(spark, dim_account_path)

print("DimAccount is ready for SCD1 merge operations")


# COMMAND ----------

dim_acc_df = spark.read.format("delta").load(dim_account_path)

print("Total rows:", dim_acc_df.count())
dim_acc_df.orderBy("AccountSK").display(50, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM DATE

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

gold_path = "abfss://gold@storage4bank.dfs.core.windows.net/"

transactions_path = f"{gold_path}transactions"
dim_date_path = f"{gold_path}dim_date"

df_t = spark.read.format("parquet").load(transactions_path)

df_t.select("Transaction_Date").show(5)


# COMMAND ----------

min_date = df_t.select(F.min("Transaction_Date")).first()[0]
max_date = df_t.select(F.max("Transaction_Date")).first()[0]

print("Min date:", min_date)
print("Max date:", max_date)


# COMMAND ----------

df_dates = spark.range(0, 1).select(
    F.explode(
        F.sequence(F.lit(min_date), F.lit(max_date))
    ).alias("Date")
)


# COMMAND ----------

df_dim_date = df_dates.select(
    F.col("Date").alias("Date"),
    F.year("Date").alias("Year"),
    F.month("Date").alias("Month"),
    F.dayofmonth("Date").alias("Day"),
    F.quarter("Date").alias("Quarter"),
    F.weekofyear("Date").alias("WeekOfYear"),
    F.date_format("Date", "EEEE").alias("DayName"),
    F.date_format("Date", "MMMM").alias("MonthName"),
    F.date_format("Date", "E").alias("ShortDayName"),
    F.date_format("Date", "MMM").alias("ShortMonthName"),
    F.concat(F.lit("Q"), F.quarter("Date")).alias("QuarterName"),
    F.concat(F.year("Date"), F.lit("-"), F.format_string("%02d", F.month("Date"))).alias("YearMonth"),
    (F.dayofyear("Date")).alias("DayOfYear"),
    (F.dayofweek("Date")).alias("DayOfWeek"),
    (F.weekofyear("Date")).alias("ISOWeek"),
    (F.date_format("Date", "yyyyMMdd")).cast("int").alias("DateKey")
)


# COMMAND ----------

df_dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_date_path)

print("DimDate overwritten successfully with new schema")


# COMMAND ----------

df_dim_date_check = spark.read.format("delta").load(dim_date_path)

print("Total rows:", df_dim_date_check.count())
df_dim_date_check.orderBy("Date").display(50, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Transactions

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

gold_path = "abfss://gold@storage4bank.dfs.core.windows.net/"
silver_path = "abfss://silver@storage4bank.dfs.core.windows.net/"

transactions_path = f"{gold_path}transactions"
dim_customer_path = f"{gold_path}dim_customer"
dim_account_path = f"{gold_path}dim_account"
dim_date_path = f"{gold_path}dim_date"
fact_transactions_path = f"{gold_path}fact_transactions"

silver_account_path = f"{silver_path}account"


# COMMAND ----------

df_t = spark.read.format("parquet").load(transactions_path)
df_cust = spark.read.format("delta").load(dim_customer_path).filter("CurrentFlag = true")
df_acc = spark.read.format("delta").load(dim_account_path)
df_date = spark.read.format("delta").load(dim_date_path)

df_silver_acc = spark.read.format("parquet").load(silver_account_path)


# COMMAND ----------

df_acc_map = df_silver_acc.select(
    F.col("AccountNumber").cast("string").alias("AccountNumber"),
    "CustomerID"
).dropDuplicates()


# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy(
    F.col("AccountNumber"),
    F.col("Transaction_Date"),
    F.col("Transaction_Time")
).orderBy(F.lit(1))

df_tx = df_t.withColumn(
    "txn_row_number", F.row_number().over(w)
).withColumn(
    "TransactionID",
    F.sha2(F.concat_ws("|",
                       F.col("AccountNumber").cast("string"),
                       F.col("Transaction_Date").cast("string"),
                       F.col("Transaction_Time").cast("string"),
                       F.col("txn_row_number").cast("string")),
           256)
).withColumn(
    "TxnDateTime",
    F.to_timestamp(F.concat_ws(" ", "Transaction_Date", "Transaction_Time"))
).withColumn(
    "DateKey",
    F.date_format("Transaction_Date", "yyyyMMdd").cast("int")
).withColumn(
    "Year", F.year("Transaction_Date")
).withColumn(
    "Month", F.month("Transaction_Date")
)


# COMMAND ----------

df_tx = df_tx.join(
    df_acc_map,
    on="AccountNumber",
    how="left"
)


# COMMAND ----------

df_tx = df_tx.join(
    df_cust.select("CustomerID", "CustomerSK"),
    on="CustomerID",
    how="left"
)


# COMMAND ----------

df_tx = df_tx.join(
    df_acc.select(
        F.col("AccountNumber").alias("acc_num_dim"),
        "AccountSK"
    ),
    df_tx.AccountNumber == F.col("acc_num_dim"),
    "left"
).drop("acc_num_dim")


# COMMAND ----------

df_tx.select("AccountNumber", "AccountSK").show(20, truncate=False)


# COMMAND ----------

df_tx = df_tx.join(
    df_date.select("DateKey", "Date"),
    on="DateKey",
    how="left"
)


# COMMAND ----------

df_fact = df_tx.select(
    "TransactionID",
    "CustomerSK",
    "AccountSK",
    "DateKey",
    "TxnDateTime",
    "Transaction_Date",
    "Transaction_Time",
    "Year",
    "Month",
    "Amount",
    "Channel",
    F.col("Transaction_Type").alias("TransactionType"),
    F.col("Transaction_Status").alias("TransactionStatus")
)


# COMMAND ----------

df_tx.select("AccountNumber", "AccountSK").distinct().show(50, truncate=False)


# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, fact_transactions_path):

    df_fact.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("Year","Month") \
        .save(fact_transactions_path)

    print("FactTransactions CREATED")

else:
    print("FactTransactions exists — SKIPPING create")


# COMMAND ----------

if DeltaTable.isDeltaTable(spark, fact_transactions_path):

    dt_fact = DeltaTable.forPath(spark, fact_transactions_path)

    dt_fact.alias("t").merge(
        df_fact.alias("s"),
        "t.TransactionID = s.TransactionID"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("MERGE complete")


# COMMAND ----------

df_final = spark.read.format("delta").load(fact_transactions_path)

print("Total rows:", df_final.count())
df_final.orderBy("TransactionID").display(50, truncate=False)


# COMMAND ----------

df_final.select(
    "TransactionID",
    "CustomerSK",
    "AccountSK",
    "DateKey"
).display(50, truncate=False)


# COMMAND ----------

fact_transactions_path = "abfss://gold@storage4bank.dfs.core.windows.net/fact_transactions"

import os
print("Exists:", DeltaTable.isDeltaTable(spark, fact_transactions_path))


# COMMAND ----------

df_fact_check = spark.read.format("delta").load(fact_transactions_path)
df_fact_check.printSchema()
df_fact_check.display(20, truncate=False)
print("Rows:", df_fact_check.count())


# COMMAND ----------

df_final = spark.read.format("delta").load(fact_transactions_path)
df_final.count()


# COMMAND ----------

spark.read.format("delta").load(dim_customer_path).count()
spark.read.format("delta").load(dim_account_path).count()
spark.read.format("delta").load(dim_date_path).count()


# COMMAND ----------

# MAGIC %sql
# MAGIC USE msk_1836173769360033.bank_dw;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE msk_1836173769360033.bank_dw;
# MAGIC
# MAGIC CREATE TABLE fact_transactions
# MAGIC AS SELECT * FROM delta.`abfss://gold@storage4bank.dfs.core.windows.net/fact_transactions`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE msk_1836173769360033.bank_dw;
# MAGIC
# MAGIC SHOW TABLES;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM fact_transactions;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_transactions LIMIT 20;
# MAGIC

# COMMAND ----------

