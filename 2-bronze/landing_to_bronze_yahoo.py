# Databricks notebook source
# MAGIC %run ../util/add_workspace

# COMMAND ----------

# MAGIC %run ./create_table

# COMMAND ----------

import pyspark.sql.functions as sf

# COMMAND ----------

from util import base_functions

# COMMAND ----------

schema = spark.table('bronze.yahoo').schema

# COMMAND ----------

df_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .schema(schema)
    .load(f'/Volumes/{environment}/landing/yahoo/*/*/*/*/*/*/*/*.csv')
)

# COMMAND ----------

df_writer = (
    df_stream
    .withColumn('filepath', sf.col('_metadata.file_path'))
    .withColumn('ingestion', sf.current_timestamp())
    .withColumn('splitedFilepath', sf.split(sf.col('filepath'), '/'))
    .withColumn('ticker', sf.col('splitedFilepath')[sf.size('splitedFilepath')-1])
    .withColumn('ticker', sf.split('ticker', '\\.')[0])
    .drop('splitedFilepath')
)

# COMMAND ----------

df_writer.writeStream\
    .trigger(availableNow=True)\
    .option("checkpointLocation", f"/Volumes/{environment}/bronze/checkpoints/yahoo")\
    .foreachBatch(lambda df, _: base_functions.upsert_to_delta(df, 'bronze.yahoo', ['date', 'ticker']))\
    .start()
