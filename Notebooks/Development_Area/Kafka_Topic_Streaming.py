# Databricks notebook source
# MAGIC %md
# MAGIC ### Reference docs:
# MAGIC * [Ploty 3d scatter plot](https://docs.databricks.com/notebooks/visualizations/plotly.html)
# MAGIC * [Arbitrary files from repos](https://docs.databricks.com/repos/work-with-notebooks-other-files.html#programmatically-read-files-from-a-repo)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import os

# COMMAND ----------

# DBTITLE 1,Topic Schema
#Load the sample event to infer the schema
sampleDF = spark.read.format("json").load(f"file:{os.getcwd()}/dts-test1.json")
schema = sampleDF.schema.json()

# COMMAND ----------

# DBTITLE 1,Setup
#params
startingOffsets = "latest" 
kafka_bootstrap_servers_plaintext = "35.86.112.176:9092"
topic = "dts-test1"

#open stream
kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .load())

#read stream to dataframe for parsing
read_stream = kafka.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), schema).alias("json")).select("json.*").select("timestamp", "well", "coordinates", df.colRegex("`^seg.*`"))

# COMMAND ----------

# DBTITLE 1,Delta Tables
#params
path = "/FileStore/Users/andrij.demianczuk@databricks.com/tmp/kafka"
checkpointPath = "/FileStore/Users/andrij.demianczuk@databricks.com/tmp/kafka_cp"

#stream to unbounded table called ad_dts_raw
read_stream.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpointPath).toTable("field_demos.canwest_sa.ad_dts_json")

# COMMAND ----------

# DBTITLE 1,Table Read
df = spark.table("field_demos.canwest_sa.ad_dts_json").select("timestamp", "well", "coordinates", df.colRegex("`^seg.*`"))
