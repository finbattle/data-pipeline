# Databricks notebook source
# MAGIC %md
# MAGIC # Select Environment

# COMMAND ----------

environment = dbutils.widgets.get('environment').lower()

# COMMAND ----------

allowed_environments = ['dev', 'prod']

# COMMAND ----------

assert environment in allowed_environments, f"invalid environment: '{environment}', allowed values are: {allowed_environments}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {environment}")

# COMMAND ----------

spark.sql(f"USE CATALOG {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS landing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS landing.cvm

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS landing.yahoo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS bronze.checkpoints

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS silver.checkpoints
