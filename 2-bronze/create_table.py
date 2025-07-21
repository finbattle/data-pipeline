# Databricks notebook source
# MAGIC %run ../util/create_workspace

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.yahoo (
# MAGIC   `date` timestamp,
# MAGIC   open decimal(30, 10),
# MAGIC   high decimal(30, 10),
# MAGIC   low decimal(30, 10),
# MAGIC   close decimal(30, 10),
# MAGIC   `volume` long,
# MAGIC   dividends decimal(30, 10),
# MAGIC   stock_splits decimal(30, 10),
# MAGIC   ticker string,
# MAGIC   filepath string,
# MAGIC   ingestion timestamp,
# MAGIC   PRIMARY KEY (
# MAGIC     ticker,
# MAGIC     `date`
# MAGIC   )
# MAGIC ) using delta
