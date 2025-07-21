# Databricks notebook source
# MAGIC %pip install yfinance
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../util/add_workspace

# COMMAND ----------

# MAGIC %run ../util/create_workspace

# COMMAND ----------

import yfinance as yf
import datetime
import pathlib

# COMMAND ----------

ticker = dbutils.widgets.get("ticker")
period = dbutils.widgets.get("period")

# COMMAND ----------

yf_obj = yf.Ticker(ticker)
df = yf_obj.history(period=period)
df['Date'] = df.index
df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']]

# COMMAND ----------

now = datetime.datetime.now()
prefix = now.strftime('%Y/%m/%d/%H/%M/%S')
directory = f'/Volumes/{environment}/landing/yahoo/{ticker}/{prefix}'
pathlib.Path(directory).mkdir(exist_ok=True, parents=True)

# COMMAND ----------

df.to_csv(f'{directory}/{ticker}.csv', index=False)
