# Databricks notebook source
# MAGIC %run ../util/add_workspace

# COMMAND ----------

# MAGIC %run ./create_table

# COMMAND ----------

import pyspark.sql.functions as sf

# COMMAND ----------

from util import base_functions

# COMMAND ----------

decimal_precision = 7
multiplier = sf.lit(10 ** decimal_precision)

df_stream = (
    spark
    .readStream
    .option("header", "true")
    .table("bronze.yahoo")
)

# COMMAND ----------

df_writer = (
    df_stream
    .select(
        sf.col("date"),
        sf.col("ticker"),
        (sf.col("open") * multiplier).cast("LONG").alias("open"),
        (sf.col("high") * multiplier).cast("LONG").alias("high"),
        (sf.col("low") * multiplier).cast("LONG").alias("low"),
        (sf.col("close") * multiplier).cast("LONG").alias("close"),
        (sf.col("volume") * multiplier).cast("LONG").alias("volume"),
        (sf.col("dividends") * multiplier).cast("LONG").alias("dividends"),
        (sf.col("stock_splits") * multiplier).cast("LONG").alias("stock_splits"),
        sf.lit(decimal_precision).alias("decimal_precision"),
    )
)

# COMMAND ----------

df_writer.writeStream\
    .trigger(availableNow=True)\
    .option("checkpointLocation", f"/Volumes/{environment}/silver/checkpoints/yahoo")\
    .foreachBatch(lambda df, _: base_functions.upsert_to_delta(df, 'silver.yahoo', ['date', 'ticker']))\
    .start()
