# Databricks notebook source
# MAGIC %run ../util/create_workspace

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.yahoo (
# MAGIC   `date` timestamp not null,
# MAGIC   ticker string not null,
# MAGIC   open long not null,
# MAGIC   high long not null,
# MAGIC   low long not null,
# MAGIC   close long not null,
# MAGIC   `volume` long not null,
# MAGIC   dividends long not null,
# MAGIC   stock_splits long not null,
# MAGIC   decimal_precision byte not null,
# MAGIC   PRIMARY KEY (
# MAGIC     ticker,
# MAGIC     `date`
# MAGIC   )
# MAGIC ) using delta
